"use strict"

import WebSocket from "ws"
import tls from "tls"
import net from "net"
import https from "https"
import fs from "fs"
import opus from "@discordjs/opus"
import HighResolutionTimer from "./HighResTimer"
import EstablishPeerConnection from "./webrtcconnection"
import PacketDataStream from "./PacketDataStream"
const { RTCAudioSink, RTCAudioSource } = require("wrtc").nonstandard

const murmurHost = "default.mumble.prod.hearo.live"
const murmurPort = 64738
const webRtcPort = 8136

var logging = false

const args = process.argv.slice(2)
if (args[0] === "log") {
  console.log("Logging enabled")
  logging = true
} else if (args.length) {
  console.log("Unknown argument(s):", ...args)
  process.exit()
}

console.log("Starting webrtc-mummur-proxy")

process.on('uncaughtException', function (exception) {
  console.log(exception);
});

process.on('unhandledRejection', (reason, p) => {
  console.log("Unhandled Rejection at: Promise ", p, " reason: ", reason);
});

process.on('warning', (warning) => {
  console.warn(warning.name);
  console.warn(warning.message);
  console.warn(warning.stack);
});

const webServer = https.createServer({cert: fs.readFileSync("cert.pem"), key: fs.readFileSync("key.pem")}, (req, res) => {    //Running on Murmur server

  //We don"t expect to get HTTP requests, only WebSocket requests
  console.log("Warning: Received HTTP request on signaling server:", req.method, req.url)

}).listen(webRtcPort, () => {

  //Create WS server
  const wsServer = new WebSocket.Server({server: webServer});
  console.log("WebSocket listening on port", webRtcPort)

  wsServer.on("connection", webSocket => {
    let clientReady = false
    let murmurSocket = null
    let dataChannel = null
    let audioSink = null
    let tracks = {}

    webSocket.sendJson = obj => webSocket.send(JSON.stringify(obj))

    const id = generateId()
    const log = (msg, ...args) => {
      if (logging) {
        console.log(`[${id}]`, msg, ...args)
      }
    }
    
    EstablishPeerConnection(webSocket, log, peerConnection => {

      peerConnection.ontrack = evt => {
        log("got track")
        audioSink = new RTCAudioSink(evt.track)
        audioSink.ondata = processAudioData
      }

      peerConnection.onClientReady = () => {
        clientReady = true
        maybeOpenMurmur()
      }
      
      const maybeOpenMurmur = () => {
        if (clientReady && dataChannel && (dataChannel.readyState === "open") && !murmurSocket) {
          murmurSocket = new tls.TLSSocket(net.createConnection(murmurPort, murmurHost, () => {
            murmurSocket.connected = true
    
            murmurSocket.on("end", () => {
              log("murmurSocket end"); 
              if (murmurSocket) {
                murmurSocket.connected = false
              }
              shutdown()
            })
          
            murmurSocket.on("error", err => {
              log("Error on Murmur socket:", err);
            });
          
            //Called every 10ms to upload audio data to the RTCAudioSource
            const uploadAudioData = (audioSource, trackData) => {
              const buffer = trackData.buffers[trackData.curBuffer]
              if (!buffer) {
                return
              }
              if (trackData.bufferPos + 480 <= buffer.length) {
                const samplesData = {
                  samples: new Uint16Array(buffer.subarray(trackData.bufferPos, trackData.bufferPos + 480)),
                  sampleRate: 48000
                }
                audioSource.onData(samplesData)
                trackData.bufferPos += 480
                if (trackData.bufferPos === buffer.length) {
                  trackData.buffers[trackData.curBuffer] = null
                  trackData.curBuffer = (trackData.curBuffer + 1) % trackData.buffers.length
                  trackData.bufferPos = 0
                }
              } else {
                log("ERROR: Partial data in upload buffer")
              }
            }

            const handleAudioFromMurmur = pds => {
              const typeTarget = pds.getByte()
              if (typeTarget !== 128) {
                log("ERROR: Unexpected type/target value:", typeTarget)
                return
              }
              const sessionId = pds.getVarint()
              const sequenceNumber = pds.getVarint()
              //console.log("audio packet:", "length =", pds.length, "(", pds.length - pds.offset, ")", "type/target =", typeTarget, "sessionId =", sessionId, "sequenceNumber =", sequenceNumber)
              let opusLength = pds.getVarint()
              if (opusLength & 0x2000) {    //Last packet flag -- ignore
                opusLength &= 0x1FFF
              }
              const remaining = pds.remaining()
              if ((remaining !== opusLength) && (remaining !== opusLength + 12)) {    //Allow for 12 bytes of position data on end
                log("ERROR: Opus data length mismatch", pds.remaining(), opusLength)
                return
              }
              let trackData = tracks[sessionId]
              if (!trackData) {
                const source = new RTCAudioSource()
                const track = source.createTrack()
                webSocket.sendJson({    //Tell the client which user this track goes with
                  type: "user",
                  sessionId: sessionId,
                  trackId: track.id
                })
                peerConnection.addTrack(track)
                tracks[sessionId] = trackData = {buffers: [null, null, null], curBuffer: 0, bufferPos: 0} //, num: trackCount++}
                trackData.intervalTimer = new HighResolutionTimer({
                  duration: 10,
                  callback: () => uploadAudioData(source, trackData)
                });
                trackData.intervalTimer.run()
                trackData.encoder = new opus.OpusEncoder(48000, 1)
              } 
              const opusData = new Uint8Array(pds.buffer.buffer, pds.offset, opusLength)
              const pcmData = new Uint16Array(trackData.encoder.decode(opusData).buffer)
              let i
              for (i = 0; i < trackData.buffers.length; i++) {
                const bufNum = (trackData.curBuffer + i) % trackData.buffers.length
                if (trackData.buffers[bufNum] === null) {
                  trackData.buffers[bufNum] = pcmData
                  break
                }
              }
              if (i === trackData.buffers.length) {
                log("WARNING: all buffers full")
              }
            }

            let startIndex = 0
            let audioPacket = null    //Used to assemble one audio packet from two data events
            let sendCount = 0
            let leftover = null

            murmurSocket.on("data", murmurData => {

              if (leftover) {   //Previous chunk didn't have enough data left to read type & length, so add that bit onto this chunk
                const leftoverLength = leftover.remaining()
                const newMurmurData = new Uint8Array(leftoverLength + murmurData.byteLength)
                newMurmurData.set(leftover.buffer.subarray(leftover.offset))
                newMurmurData.set(murmurData, leftoverLength)
                murmurData = newMurmurData
                leftover = null
              }

              if (audioPacket) {     //Part-way through an audio packet
                const length = pds.getInt32(2)    //Get length from start of packet
                const bytesNeeded = length - (audioPacket.offset - 6)
                audioPacket.set(murmurData.subarray(0, bytesNeeded))
                audioPacket.offset = 6
                handleAudioFromMurmur(audioPacket)
                audioPacket = null
                if (bytesNeeded === murmurData.length) {    //We used up all the data
                  return
                }
                murmurData = murmurData.subarray(bytesNeeded)
              }

              const pds = new PacketDataStream(murmurData, startIndex)
              startIndex = 0

              while (true) {

                const remaining = pds.remaining()
                if (remaining < 6) {      //Not enough data to read type & length
                  if (pds.offset > 0) {   //Send data up to this point
                    dataChannel.send(murmurData.subarray(0, pds.offset))
                  }
                  leftover = pds          //Save the leftover part
                  break
                }

                let type = pds.getInt16()
                let length = pds.getInt32()

                if (type === 1) {       //Audio packet

                  //If we're not at the start of buffer, send the part before
                  if (pds.offset > 6) {
                    dataChannel.send(murmurData.subarray(0, pds.offset - 6))
                  }
                  if (pds.offset + length > murmurData.length) {
                    audioPacket = new PacketDataStream(length + 6)
                    pds.skip(-6)
                    audioPacket.putBytes(pds.remainder())
                    break
                  }
                  const saveOffset = pds.offset
                  handleAudioFromMurmur(pds)
                  if (saveOffset + length === murmurData.length) {
                    break
                  }
                  murmurData = murmurData.subarray(saveOffset + length)
                  pds.reset(murmurData)

                } else {    //Not an audio packet

                  const remaining = pds.remaining()
                  if (length < remaining) {
                    pds.skip(length)
                  } else {  //End of the buffer, so send data
                    dataChannel.send(murmurData)       //Send the data
                    sendCount++
                    startIndex = length - remaining
                    break
                  }
                }
              }
            })
    
            webSocket.sendJson({type: "start"})
          }))
        }
      }

      //
      // Create connections
      //
    
      dataChannel = peerConnection.createDataChannel("dataChannel")

      dataChannel.onopen = () => {
        log(`dataChannel open (state = ${dataChannel.readyState})`)
        maybeOpenMurmur()
      }

      dataChannel.onmessage = evt => {
        if (murmurSocket && murmurSocket.connected) {
          murmurSocket.write(Buffer.from(evt.data))
        } else {
          log("Got dataChannel message before murmurSocket is connected")
        }
      }

      //
      // Shutdown
      //
    
      let shuttingDown = false
      const shutdown = () => {
        if (!shuttingDown) {
          shuttingDown = true
          log("shutdown")
          if (murmurSocket) {
            murmurSocket.end()
            murmurSocket = null
          }
          webSocket.close()
          if (dataChannel) {
            dataChannel.close()
            dataChannel = null
          }
          peerConnection.close()
        }
      }
    
      //
      // Handle disconnections & errors
      //
    
      dataChannel.onclose = shutdown
    
      dataChannel.onerror = err => {
        log("dataChannel error:", err)
      }
      
      //
      // Handle incoming data
      //
    
      //For encoding and sending audio data
      const opusEncoder = new opus.OpusEncoder(48000, 1)
      let rawDataBuffer = new Uint16Array(480 * 5)
      let rawDataBufferOffset = 0
      peerConnection.packetCount = 0
      let lastSampleCount

      const processAudioData = data => {
        if (!murmurSocket || !murmurSocket.connected) {
          return
        }
    
        if (data.samples.length > 480) {
          throw new Error("Too many samples in packet: " + data.samples.length)
        }
    
        if (lastSampleCount && (data.samples.length > lastSampleCount) && rawDataBufferOffset) {
          log("discarding (size change)", rawDataBufferOffset)
          rawDataBufferOffset = 0
        }
        lastSampleCount = data.samples.length

        new Uint16Array(rawDataBuffer.buffer, rawDataBufferOffset * 2, data.samples.length).set(data.samples)
        rawDataBufferOffset += data.samples.length
    
        if (rawDataBufferOffset > 1920) {
          log("discarding (too big)", rawDataBufferOffset)
          rawDataBufferOffset = 0
        }
    
        if (rawDataBufferOffset === 1920) {
    
          //Convert raw samples to Opus
          const encodedSamples = opusEncoder.encode(new Uint16Array(rawDataBuffer.buffer, 0, rawDataBufferOffset));
    
          //Build TCP tunnel packet
          const pds = new PacketDataStream(2 + 4 + 1 + 4 + 4 + encodedSamples.byteLength + 12)      //Allow for up to four bytes for varints
           
          pds.putInt16(1)                             //16-bit int: Packet type 1 = UDP Tunnel
          pds.skip(4)                                 //Leave space for 32-bit in packet length
          pds.putByte(128)                            //8-bit int: UDP packet header -- Packet type (OPUS) and target (0) 
          pds.putVarint(peerConnection.packetCount)   //Varint: Sequence number
          pds.putVarint(encodedSamples.byteLength)    //Varint: Length of the raw data
          pds.putBytes(encodedSamples)                //Byte array of encoded audio
          pds.putInt32(pds.offset - 6, 2)             //Save the packet length (less the type & length part) back at position 2 in the buffer
    
          //Send it off!
          murmurSocket.write(Buffer.from(new Uint8Array(pds.buffer.buffer, 0, pds.offset)))

          //Reset to start of buffer
          rawDataBufferOffset = 0
        }
        peerConnection.packetCount += 1
      }
    })
  })
})

//
// Function to generate id numbers
//

const digit0 = [15, 12, 5, 6, 4, 9, 0, 1, 2, 10, 13, 11, 3, 8, 14, 7]
const digit1 = [11, 3, 14, 4, 13, 1, 5, 7, 6, 8, 10, 9, 0, 15, 12, 2]
const digit2 = [15, 1, 8, 12, 5, 11, 6, 7, 10, 14, 4, 9, 13, 0, 3, 2]
const digit3 = [4, 11, 13, 2, 5, 3, 9, 6, 8, 15, 14, 1, 7, 10, 0, 12]

let counter0 = Math.floor(Math.random() * 16)
let counter1 = Math.floor(Math.random() * 16)
let counter2 = Math.floor(Math.random() * 16)
let counter3 = Math.floor(Math.random() * 16)

function generateId() {

  const result = (digit3[counter3].toString(16) + digit2[counter2].toString(16) + digit1[counter1].toString(16) + digit0[counter0].toString(16)).toUpperCase()

  counter0 = (counter0 + 1) % 16
  counter1 = (counter1 + 1) % 16
  counter2 = (counter2 + 1) % 16
  counter3 = (counter3 + 1) % 16

  if (counter0 === 0) {
    counter1++;
    if (counter1 === 16) {
      counter1 = 0
      counter2++
      if (counter2 === 16) {
        counter2 = 0
        counter3++
      }
    }
  }

  return result
}

