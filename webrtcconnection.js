'use strict';

const RTCPeerConnection = require('wrtc').RTCPeerConnection;

const TIME_TO_CONNECTED = 20000;
const TIME_TO_RECONNECTED = 20000;

function EstablishPeerConnection(signalingSocket, log, lostTrackCallback, beforeOffer) {
  let timeoutTimer

  const close = () => {
    if (timeoutTimer) {
      clearTimeout(timeoutTimer)
    }
    peerConnection.close()
  }

  const doSetTimeout = time => {
    timeoutTimer = setTimeout(() => {
      if ((peerConnection.iceConnectionState !== 'connected') && (peerConnection.iceConnectionState !== 'completed')) {
        close()
      }
    }, time)
  }

  const peerConnection = new RTCPeerConnection({
    sdpSemantics: "unified-plan",
    iceServers: [
      {
        'urls': 'stun:stun.l.google.com:19302'
      },
    ]
  });

  peerConnection.oniceconnectionstatechange = () => {
    log("iceConnectionState", peerConnection.iceConnectionState)
    if ((peerConnection.iceConnectionState === 'disconnected') || (peerConnection.iceConnectionState === 'failed')) {
      doSetTimeout(TIME_TO_RECONNECTED)
    }
  }

  peerConnection.onconnectionstatechange = () => {
    log("connectionState", peerConnection.connectionState)
    if ((peerConnection.connectionState === "disconnected") || (peerConnection.connectionState === "failed")) {
      close()
    }
  }

  peerConnection.onicegatheringstatechange = () => {
    log("iceGatheringState:", peerConnection.iceGatheringState)
  }

  peerConnection.onsignalingstatechange = () => {
    log("signalingState:", peerConnection.signalingState)
  }

  peerConnection.onnegotiationneeded = () => {
    log("onnegotiationneeded")
    peerConnection.createOffer()
      .then(offer => {
        if (peerConnection.signalingState !== "stable") {   //In case we received an offer while we were waiting for ours to be created
          log("aborting offer")
          return
        } 
        peerConnection.setLocalDescription(offer)
          .then(() => signalingSocket.sendJson({
            offer: peerConnection.localDescription
          }))
      })
      .catch(err => log("Error in onnegotiationneeded:", err))
  }

  peerConnection.onicecandidate = evt => {
    if (evt.candidate && evt.candidate.candidate) {
      //log("on icecandidate")
      signalingSocket.sendJson({
        ice: {
          candidate: evt.candidate.candidate,
          sdpMid: evt.candidate.sdpMid,
          sdpMLineIndex: evt.candidate.sdpMLineIndex
        }
      })
    }
  }

  beforeOffer(peerConnection)

  doSetTimeout(TIME_TO_CONNECTED)

  signalingSocket.on("message", msg => {
    let json

    try {
      json = JSON.parse(msg)
    }
    catch (err) {
      log("JSON parsing error: " + msg.toString())
      return
    }

    if (json.answer) {
      log("answer received")
      if (json.answer.type !== "answer") {
        log("ERROR: Wrong type for answer:", json.answer.type)
      } else {
        peerConnection.setRemoteDescription(json.answer)
      }
    }

    if (json.offer) {
      log("offer received")
      if (json.offer.type !== "offer") {
        log("ERROR: Wrong type for offer:", json.offer.type)
      } else {
        if (peerConnection.signalingState === "stable") {    //If we've already sent out an offer, ignore this one.  Client will do rollback.
          peerConnection.setRemoteDescription(json.offer)
            .then(() => peerConnection.createAnswer())
            .then(answer => {
              peerConnection.setLocalDescription(answer)
                .then(() => {
                  signalingSocket.sendJson({answer: answer})  //Should be able to send localDescription except for Firefox bug
                })
            })
            .catch(err => log("Error responding to offer:", err))
        }
      }
    }

    if (json.ice) {
      //log("adding iceCandidate")
      if (json.ice.candidate) {
        peerConnection.addIceCandidate(json.ice)
          .catch(err => {
            log("Error setting ICE candidate:", err)
          })
      }
    }

    if (json.ready || (json.type === "ready")) {
      log("client ready")
      if (peerConnection.onClientReady) {
        peerConnection.onClientReady()
      }
    }

    if (json.lostTrack) {
      lostTrackCallback(json.lostTrack)
    }
    
    if (json.close || (json.type === "close")) {
      log("closing peerConnection")
      close()
    }
  })

  return peerConnection;
}

module.exports.EstablishPeerConnection = EstablishPeerConnection