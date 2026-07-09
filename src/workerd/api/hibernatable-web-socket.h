// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/api/basics.h>
#include <workerd/api/hibernation-event-params.h>
#include <workerd/api/web-socket.h>
#include <workerd/io/trace.h>
#include <workerd/io/worker-interface.capnp.h>
#include <workerd/io/worker-interface.h>
#include <workerd/io/worker.h>

#include <kj/debug.h>

namespace workerd::api {

using HibernationReader =
    rpc::HibernatableWebSocketEventDispatcher::HibernatableWebSocketEventParams::Reader;
struct HibernatableWebSocketCustomEventTestAccess;
class HibernatableWebSocketEvent final: public ExtendableEvent {
 public:
  explicit HibernatableWebSocketEvent();

  static jsg::Ref<HibernatableWebSocketEvent> constructor(kj::String type) = delete;

  // When we call a close or error event, we need to move the owned websocket and the tags back into
  // the api::WebSocket to extend their lifetimes. This is because the HibernatableWebSocket, which
  // has owned these things for the entire duration of the connection, is free to go away after we
  // dispatch the final event. JS may still want to access the underlying kj::WebSocket or the tags,
  // so we have to transfer ownership to JS-land.
  struct ItemsForRelease {
    jsg::Ref<WebSocket> webSocketRef;
    kj::Own<kj::WebSocket> ownedWebSocket;
    kj::Array<kj::String> tags;

    explicit ItemsForRelease(
        jsg::Ref<WebSocket> ref, kj::Own<kj::WebSocket> owned, kj::Array<kj::String> tags);
  };

  // Call this when transferring ownership of the kj::WebSocket and tags to the api::WebSocket.
  //
  // Gets a reference to the api::WebSocket, and moves the owned kj::WebSocket out of the
  // HibernatableWebSocket whose event we are currently delivering.
  ItemsForRelease prepareForRelease(jsg::Lock& lock, kj::StringPtr websocketId);

  // Should only be called once per event, see definition for details.
  jsg::Ref<WebSocket> claimWebSocket(jsg::Lock& lock, kj::StringPtr websocketId);

  JSG_RESOURCE_TYPE(HibernatableWebSocketEvent) {
    JSG_INHERIT(ExtendableEvent);
  }
};

class HibernatableWebSocketCustomEvent final: public WorkerInterface::CustomEvent,
                                              public kj::Refcounted {
 public:
  // Local wake events hold an owning manager ref. RPC wake events cannot carry a C++ reference, so
  // they pass kj::none and run() falls back to LegacyHibernationManagerImpl::findManagerForEvent()
  // using the event's WebSocket ID.
  HibernatableWebSocketCustomEvent(uint16_t typeId,
      kj::Own<HibernationReader> params,
      kj::Maybe<Worker::Actor::HibernationManager&> manager = kj::none);
  HibernatableWebSocketCustomEvent(
      uint16_t typeId, HibernatableSocketParams params, Worker::Actor::HibernationManager& manager);

  kj::Promise<Result> run(kj::Own<IoContext_IncomingRequest> incomingRequest,
      kj::Maybe<kj::StringPtr> entrypointName,
      kj::Maybe<Worker::VersionInfo> versionInfo,
      Frankenvalue props,
      kj::TaskSet& waitUntilTasks,
      bool isDynamicDispatch) override;

  kj::Promise<Result> sendRpc(capnp::HttpOverCapnpFactory& httpOverCapnpFactory,
      capnp::ByteStreamFactory& byteStreamFactory,
      FrankenvalueHandler& frankenvalueHandler,
      rpc::EventDispatcher::Client dispatcher) override;

  uint16_t getType() override {
    return typeId;
  }

  tracing::EventInfo getEventInfo() const override;

  kj::Promise<Result> notSupported() override {
    KJ_UNIMPLEMENTED("hibernatable web socket event not supported");
  }

 private:
  friend struct HibernatableWebSocketCustomEventTestAccess;

  // Returns `params`, but if we have a HibernationReader we convert it to a
  // HibernatableSocketParams first.
  HibernatableSocketParams consumeParams();

  // Makes the manager that owns `websocketId` reachable for this event. It is installed on the actor
  // when the actor has none; if the actor already holds a different one -- a code-update wake
  // replaced it -- that one stays and delivery reaches the owner through the process-global registry
  // instead. Local events carry an owning ref; RPC events resolve one from the registry.
  void ensureHibernationManagerForEvent(Worker::Actor& actor, kj::StringPtr websocketId);

  // Peeks at params to extract the event type for tracing, without consuming them.
  tracing::HibernatableWebSocketEventInfo::Type getEventType() const;

  uint16_t typeId;
  kj::OneOf<HibernatableSocketParams, kj::Own<HibernationReader>> params;
  kj::Maybe<uint32_t> timeoutMs;
  kj::Maybe<kj::Own<Worker::Actor::HibernationManager>> manager;
  bool eventRegisteredGlobally = false;
};

#define EW_WEB_SOCKET_MESSAGE_ISOLATE_TYPES                                                        \
  api::HibernatableWebSocketEvent, api::HibernatableWebSocketExportedHandler
}  // namespace workerd::api
