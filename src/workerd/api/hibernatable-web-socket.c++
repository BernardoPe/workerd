// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "hibernatable-web-socket.h"

#include <workerd/api/global-scope.h>
#include <workerd/io/legacy-hibernation-manager.h>
#include <workerd/io/limit-enforcer.h>
#include <workerd/io/tracer.h>
#include <workerd/jsg/ser.h>

namespace workerd::api {

namespace {

// Hibernatable sockets bypass the regular readLoop (which marks non-hibernatable receives), so we
// mark the receive here, in-scope, from both the Text and Data dispatch branches. The enforcer
// captures the timestamp, so this side stays time-agnostic.
void markHibernatableWebSocketReceive(IoContext& context) {
  context.getWorker().getIsolate().getLimitEnforcer().markPerfEvent("ws_received"_kjc);
}

}  // namespace

HibernatableWebSocketEvent::HibernatableWebSocketEvent(): ExtendableEvent("webSocketMessage") {};

HibernatableWebSocketEvent::ItemsForRelease HibernatableWebSocketEvent::prepareForRelease(
    jsg::Lock& lock, kj::StringPtr websocketId) {
  auto& hibernatableWebSocket = LegacyHibernationManagerImpl::takeWebSocketForEvent(websocketId);
  auto websocketRef = hibernatableWebSocket.getActiveOrUnhibernate(lock);
  auto ownedWebSocket = kj::mv(KJ_REQUIRE_NONNULL(hibernatableWebSocket.ws));
  auto tags = hibernatableWebSocket.cloneTags();

  return ItemsForRelease(kj::mv(websocketRef), kj::mv(ownedWebSocket), kj::mv(tags));
}

jsg::Ref<WebSocket> HibernatableWebSocketEvent::claimWebSocket(
    jsg::Lock& lock, kj::StringPtr websocketId) {
  // Should only be called once per event since it removes the HibernatableWebSocket from the
  // webSocketsForEventHandler collection.
  auto& hibernatableWebSocket = LegacyHibernationManagerImpl::takeWebSocketForEvent(websocketId);
  return hibernatableWebSocket.getActiveOrUnhibernate(lock);
}

kj::Promise<WorkerInterface::CustomEvent::Result> HibernatableWebSocketCustomEvent::run(
    kj::Own<IoContext_IncomingRequest> incomingRequest,
    kj::Maybe<kj::StringPtr> entrypointName,
    kj::Maybe<Worker::VersionInfo> versionInfo,
    Frankenvalue props,
    kj::TaskSet& waitUntilTasks,
    bool isDynamicDispatch) {
  // Mark the request as delivered because we're about to run some JS.
  auto& context = incomingRequest->getContext();
  incomingRequest->delivered();

  KJ_DEFER({ incomingRequest->drain(waitUntilTasks, kj::mv(incomingRequest)); });

  EventOutcome outcome = EventOutcome::OK;

  auto eventParameters = consumeParams();

  try {
    eventRegisteredGlobally = false;
    ensureHibernationManagerForEvent(
        KJ_REQUIRE_NONNULL(context.getActor()), eventParameters.websocketId);
    auto eventManager = KJ_REQUIRE_NONNULL(manager)->addRef();
    kj::Maybe<kj::String> eventWebsocketId;
    if (eventRegisteredGlobally) {
      eventWebsocketId = kj::str(eventParameters.websocketId);
    }
    KJ_DEFER({
      if (eventWebsocketId != kj::none) {
        kj::downcast<LegacyHibernationManagerImpl>(*eventManager)
            .cancelEvent(KJ_REQUIRE_NONNULL(eventWebsocketId));
      }
    });

    co_await context.run(
        [entrypointName = entrypointName, eventParameters = kj::mv(eventParameters),
            versionInfo = kj::mv(versionInfo), props = kj::mv(props),
            isDynamicDispatch](Worker::Lock& lock, IoContext& context) mutable {
      KJ_SWITCH_ONEOF(eventParameters.eventType) {
        KJ_CASE_ONEOF(text, HibernatableSocketParams::Text) {
          markHibernatableWebSocketReceive(context);
          return lock.getGlobalScope().sendHibernatableWebSocketMessage(context,
              kj::mv(text.message), eventParameters.eventTimeoutMs,
              kj::mv(eventParameters.websocketId), lock,
              lock.getExportedHandler(entrypointName, kj::mv(versionInfo), kj::mv(props),
                  context.getActor(), isDynamicDispatch));
        }
        KJ_CASE_ONEOF(data, HibernatableSocketParams::Data) {
          markHibernatableWebSocketReceive(context);
          return lock.getGlobalScope().sendHibernatableWebSocketMessage(context,
              kj::mv(data.message), eventParameters.eventTimeoutMs,
              kj::mv(eventParameters.websocketId), lock,
              lock.getExportedHandler(entrypointName, kj::mv(versionInfo), kj::mv(props),
                  context.getActor(), isDynamicDispatch));
        }
        KJ_CASE_ONEOF(close, HibernatableSocketParams::Close) {
          return lock.getGlobalScope().sendHibernatableWebSocketClose(context, kj::mv(close),
              eventParameters.eventTimeoutMs, kj::mv(eventParameters.websocketId), lock,
              lock.getExportedHandler(entrypointName, kj::mv(versionInfo), kj::mv(props),
                  context.getActor(), isDynamicDispatch));
        }
        KJ_CASE_ONEOF(e, HibernatableSocketParams::Error) {
          return lock.getGlobalScope().sendHibernatableWebSocketError(context, kj::mv(e.error),
              eventParameters.eventTimeoutMs, kj::mv(eventParameters.websocketId), lock,
              lock.getExportedHandler(entrypointName, kj::mv(versionInfo), kj::mv(props),
                  context.getActor(), isDynamicDispatch));
        }
        KJ_UNREACHABLE;
      }
    });
  } catch (kj::Exception& e) {
    if (auto desc = e.getDescription();
        !jsg::isTunneledException(desc) && !jsg::isDoNotLogException(desc)) {
      LOG_EXCEPTION("HibernatableWebSocketCustomEvent"_kj, e);
    }
    incomingRequest->getMetrics().reportFailure(e);
    context.logUncaughtExceptionAsync(UncaughtExceptionSource::ASYNC_TASK, e.clone());
    outcome = EventOutcome::EXCEPTION;
  }

  co_return Result{
    .outcome = outcome,
  };
}

void HibernatableWebSocketCustomEvent::ensureHibernationManagerForEvent(
    Worker::Actor& actor, kj::StringPtr websocketId) {
  // The actor exists by this point, and needs a hibernation manager in place before any event that
  // might reach for it runs.
  KJ_IF_SOME(m, manager) {
    KJ_IF_SOME(existingManager, actor.getHibernationManager()) {
      if (&existingManager != m.get()) {
        // Local events carry a C++ manager ref directly and do not normally need the global
        // registry. If a code-update wake already installed a replacement actor manager, publish
        // this event ID so claim/prepare can still route to the hibernated socket's old manager.
        kj::downcast<LegacyHibernationManagerImpl>(*m).registerManagerForEvent(websocketId);
        eventRegisteredGlobally = true;
      }
    } else {
      actor.setHibernationManager(m->addRef());
    }
    return;
  }

  KJ_IF_SOME(registered, LegacyHibernationManagerImpl::findManagerForEvent(websocketId)) {
    // RPC-delivered events cannot carry a C++ manager reference. The event ID is authoritative;
    // retain that manager for the whole event even if this actor already has a newer manager.
    if (actor.getHibernationManager() == kj::none) {
      actor.setHibernationManager(registered->addRef());
    }
    manager = kj::mv(registered);
    eventRegisteredGlobally = true;
    return;
  }

  kj::throwRecoverableException(
      KJ_EXCEPTION(FAILED, "hibernatable WebSocket event manager was not found for this event ID"));
}

kj::Promise<WorkerInterface::CustomEvent::Result> HibernatableWebSocketCustomEvent::sendRpc(
    capnp::HttpOverCapnpFactory& httpOverCapnpFactory,
    capnp::ByteStreamFactory& byteStreamFactory,
    FrankenvalueHandler& frankenvalueHandler,
    rpc::EventDispatcher::Client dispatcher) {
  auto req = dispatcher.castAs<rpc::HibernatableWebSocketEventDispatcher>()
                 .hibernatableWebSocketEventRequest();
  kj::Maybe<kj::Own<Worker::Actor::HibernationManager>> registeredManager;
  kj::Maybe<kj::String> registeredWebsocketId;

  KJ_IF_SOME(rpcParameters, params.tryGet<kj::Own<HibernationReader>>()) {
    req.setMessage(rpcParameters->getMessage());
  } else {
    auto message = req.initMessage();
    auto payload = message.initPayload();
    auto& eventParameters = KJ_REQUIRE_NONNULL(params.tryGet<HibernatableSocketParams>());
    KJ_IF_SOME(m, manager) {
      kj::downcast<LegacyHibernationManagerImpl>(*m).registerManagerForEvent(
          eventParameters.websocketId);
      registeredManager = m->addRef();
      registeredWebsocketId = kj::str(eventParameters.websocketId);
    }
    KJ_SWITCH_ONEOF(eventParameters.eventType) {
      KJ_CASE_ONEOF(text, HibernatableSocketParams::Text) {
        payload.setText(kj::mv(text.message));
      }
      KJ_CASE_ONEOF(data, HibernatableSocketParams::Data) {
        payload.setData(kj::mv(data.message));
      }
      KJ_CASE_ONEOF(close, HibernatableSocketParams::Close) {
        auto closeBuilder = payload.initClose();
        closeBuilder.setCode(close.code);
        closeBuilder.setReason(kj::mv(close.reason));
        closeBuilder.setWasClean(close.wasClean);
      }
      KJ_CASE_ONEOF(e, HibernatableSocketParams::Error) {
        payload.setError(e.error.getDescription());
      }
      KJ_UNREACHABLE;
    }
    message.setWebsocketId(kj::mv(eventParameters.websocketId));
    KJ_IF_SOME(t, eventParameters.eventTimeoutMs) {
      message.setEventTimeoutMs(t);
    }
  }

  auto result = req.send().then([](auto resp) {
    auto respResult = resp.getResult();
    return WorkerInterface::CustomEvent::Result{
      .outcome = respResult.getOutcome(),
    };
  });

  KJ_IF_SOME(m, registeredManager) {
    return result.attach(
        kj::defer([manager = kj::mv(m),
                      websocketId = kj::mv(KJ_REQUIRE_NONNULL(registeredWebsocketId))]() mutable {
      kj::downcast<LegacyHibernationManagerImpl>(*manager).cancelEvent(websocketId);
    }));
  }
  return result;
}

HibernatableWebSocketEvent::ItemsForRelease::ItemsForRelease(
    jsg::Ref<WebSocket> ref, kj::Own<kj::WebSocket> owned, kj::Array<kj::String> tags)
    : webSocketRef(kj::mv(ref)),
      ownedWebSocket(kj::mv(owned)),
      tags(kj::mv(tags)) {}

HibernatableWebSocketCustomEvent::HibernatableWebSocketCustomEvent(uint16_t typeId,
    kj::Own<HibernationReader> params,
    kj::Maybe<Worker::Actor::HibernationManager&> manager)
    : typeId(typeId),
      params(kj::mv(params)),
      manager(manager.map(
          [](Worker::Actor::HibernationManager& manager) { return manager.addRef(); })) {}
HibernatableWebSocketCustomEvent::HibernatableWebSocketCustomEvent(
    uint16_t typeId, HibernatableSocketParams params, Worker::Actor::HibernationManager& manager)
    : typeId(typeId),
      params(kj::mv(params)),
      manager(manager.addRef()) {}

// Try to extract event type from params if available
tracing::HibernatableWebSocketEventInfo::Type HibernatableWebSocketCustomEvent::getEventType()
    const {
  KJ_SWITCH_ONEOF(params) {
    KJ_CASE_ONEOF(socketParams, HibernatableSocketParams) {
      KJ_SWITCH_ONEOF(socketParams.eventType) {
        KJ_CASE_ONEOF(_, HibernatableSocketParams::Text) {
          return tracing::HibernatableWebSocketEventInfo::Message{};
        }
        KJ_CASE_ONEOF(_, HibernatableSocketParams::Data) {
          return tracing::HibernatableWebSocketEventInfo::Message{};
        }
        KJ_CASE_ONEOF(close, HibernatableSocketParams::Close) {
          return tracing::HibernatableWebSocketEventInfo::Close{close.code, close.wasClean};
        }
        KJ_CASE_ONEOF(_, HibernatableSocketParams::Error) {
          return tracing::HibernatableWebSocketEventInfo::Error{};
        }
      }
    }
    KJ_CASE_ONEOF(reader, kj::Own<HibernationReader>) {
      // Parse the HibernationReader to determine the actual event type
      auto payload = reader->getMessage().getPayload();
      switch (payload.which()) {
        case rpc::HibernatableWebSocketEventMessage::Payload::TEXT:
        case rpc::HibernatableWebSocketEventMessage::Payload::DATA:
          return tracing::HibernatableWebSocketEventInfo::Message{};
        case rpc::HibernatableWebSocketEventMessage::Payload::CLOSE: {
          auto close = payload.getClose();
          return tracing::HibernatableWebSocketEventInfo::Close{
            close.getCode(), close.getWasClean()};
        }
        case rpc::HibernatableWebSocketEventMessage::Payload::ERROR:
          return tracing::HibernatableWebSocketEventInfo::Error{};
      }
    }
  }
  KJ_UNREACHABLE;
}

tracing::EventInfo HibernatableWebSocketCustomEvent::getEventInfo() const {
  return tracing::HibernatableWebSocketEventInfo(getEventType());
}

HibernatableSocketParams HibernatableWebSocketCustomEvent::consumeParams() {
  KJ_IF_SOME(p, params.tryGet<kj::Own<HibernationReader>>()) {
    kj::Maybe<HibernatableSocketParams> eventParameters;
    auto websocketId = kj::str(p->getMessage().getWebsocketId());
    auto payload = p->getMessage().getPayload();
    switch (payload.which()) {
      case rpc::HibernatableWebSocketEventMessage::Payload::TEXT: {
        eventParameters.emplace(kj::str(payload.getText()), kj::mv(websocketId));
        break;
      }
      case rpc::HibernatableWebSocketEventMessage::Payload::DATA: {
        kj::Array<byte> b = kj::heapArray(payload.getData().asBytes());
        eventParameters.emplace(kj::mv(b), kj::mv(websocketId));
        break;
      }
      case rpc::HibernatableWebSocketEventMessage::Payload::CLOSE: {
        auto close = payload.getClose();
        eventParameters.emplace(
            close.getCode(), kj::str(close.getReason()), close.getWasClean(), kj::mv(websocketId));
        break;
      }
      case rpc::HibernatableWebSocketEventMessage::Payload::ERROR: {
        eventParameters.emplace(
            KJ_EXCEPTION(FAILED, kj::str(payload.getError())), kj::mv(websocketId));
        break;
      }
    }
    KJ_REQUIRE_NONNULL(eventParameters).setTimeout(p->getMessage().getEventTimeoutMs());
    return kj::mv(KJ_REQUIRE_NONNULL(eventParameters));
  }
  return kj::mv(KJ_REQUIRE_NONNULL(params.tryGet<HibernatableSocketParams>()));
}

}  // namespace workerd::api
