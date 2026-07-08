// Copyright (c) 2017-2023 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "legacy-hibernation-manager.h"

#include "io-channels.h"
#include "io-context.h"

#include <workerd/util/uuid.h>

#include <kj/common.h>
#include <kj/mutex.h>

namespace workerd {

namespace {
// Maps each hibernatable WebSocket event ID currently being delivered to the manager that owns the
// socket. The registry is process-global because delivery can cross event loops, but the manager,
// its native WebSockets, and its JavaScript state remain bound to their originating thread.
//
// Lock-order invariant: code that needs both this registry and a manager's
// webSocketsForEventHandler lock must take this global lock first, then the per-manager lock. These
// nested critical sections are slow-path event-delivery bookkeeping and must keep the two maps
// consistent.
struct RegisteredManager {
  kj::ThreadId threadId;

  // Non-owning, and must stay that way: an owning reference would keep the manager alive for as
  // long as an event is registered, and erasing the last one (which happens under both locks, e.g.
  // in cancelEvent()) would run the destructor re-entrantly and deadlock on the non-recursive
  // mutexes. `~LegacyHibernationManagerImpl` erases its own entries, so a pointer here is always
  // live. Only findManagerForEvent() addRefs through it, and it checks `threadId` first, since
  // kj::Refcounted is not threadsafe.
  LegacyHibernationManagerImpl* manager;
};

const kj::MutexGuarded<kj::HashMap<kj::String, RegisteredManager>>&
getManagersByEventWebSocketId() {
  // Intentionally process-lifetime, to avoid depending on static destruction order during process
  // teardown: a manager outliving the registry would leave its destructor scanning freed memory.
  static const auto* const managers =
      new kj::MutexGuarded<kj::HashMap<kj::String, RegisteredManager>>();
  return *managers;
}
}  // namespace

LegacyHibernationManagerImpl::HibernatableWebSocket::HibernatableWebSocket(
    jsg::Ref<api::WebSocket> websocket,
    kj::ArrayPtr<kj::String> tags,
    LegacyHibernationManagerImpl& manager)
    : tagItems(kj::heapArray<TagListItem>(tags.size())),
      activeOrPackage(kj::mv(websocket)),
      // The `ws` starts off empty because we need to set up our tagging infrastructure before
      // calling api::WebSocket::acceptAsHibernatable(). We will transfer ownership of the
      // kj::WebSocket prior to starting the readLoop.
      ws(kj::none),
      manager(manager) {}

LegacyHibernationManagerImpl::HibernatableWebSocket::~HibernatableWebSocket() noexcept(false) {
  // We expect this dtor to be called when we're removing a HibernatableWebSocket
  // from our `allWs` collection in the HibernationManager.

  // This removal is fast because we have direct access to each kj::List, as well as direct
  // access to each TagListItem we want to remove.
  for (auto& item: tagItems) {
    KJ_IF_SOME(list, item.list) {
      // The list reference is non-null, so we still have a valid reference to this
      // TagListItem in the list, which we will now remove.
      list.remove(item);
      if (list.empty()) {
        // Remove the bucket in tagToWs if the tag has no more websockets.
        manager.tagToWs.erase(kj::mv(item.tag));
      }
    }
    item.hibWS = kj::none;
    item.list = kj::none;
  }
}

kj::Array<kj::StringPtr> LegacyHibernationManagerImpl::HibernatableWebSocket::getTags() {
  auto tags = kj::heapArray<kj::StringPtr>(tagItems.size());
  for (auto i: kj::indices(tagItems)) {
    tags[i] = tagItems[i].tag;
  }
  return tags;
}

kj::Array<kj::String> LegacyHibernationManagerImpl::HibernatableWebSocket::cloneTags() {
  auto tags = kj::heapArray<kj::String>(tagItems.size());
  for (auto i: kj::indices(tagItems)) {
    tags[i] = kj::str(tagItems[i].tag);
  }
  return tags;
}

jsg::Ref<api::WebSocket> LegacyHibernationManagerImpl::HibernatableWebSocket::
    getActiveOrUnhibernate(jsg::Lock& js) {
  KJ_IF_SOME(package, activeOrPackage.tryGet<api::WebSocket::HibernationPackage>()) {
    // Recreate our tags array for the api::WebSocket.
    package.maybeTags = getTags();

    // Now that we unhibernated the WebSocket, we can set the last received autoResponse timestamp
    // that was stored in the corresponding HibernatableWebSocket. We also move autoResponsePromise
    // from the hibernation manager to api::websocket to prevent possible ws.send races.
    activeOrPackage
        .init<jsg::Ref<api::WebSocket>>(
            api::WebSocket::hibernatableFromNative(js, *KJ_REQUIRE_NONNULL(ws), kj::mv(package)))
        ->setAutoResponseStatus(autoResponseTimestamp, kj::mv(autoResponsePromise));
    autoResponsePromise = kj::READY_NOW;
  }
  return activeOrPackage.get<jsg::Ref<api::WebSocket>>().addRef();
}

LegacyHibernationManagerImpl::LegacyHibernationManagerImpl(
    kj::Own<Worker::Actor::Loopback> loopback, uint16_t hibernationEventType)
    : loopback(kj::mv(loopback)),
      hibernationEventType(hibernationEventType),
      onDisconnect(DisconnectHandler{}),
      readLoopTasks(onDisconnect) {}

LegacyHibernationManagerImpl::~LegacyHibernationManagerImpl() noexcept(false) {
  // Drop our outstanding tasks, the `readLoopTasks` have weak references to the
  // `HibernatableWebSockets` in `allWs`, and since we're about to drop all of those WebSockets,
  // we can't allow any more events to be delivered.
  readLoopTasks.clear();

  // Note that the HibernatableWebSocket destructor handles removing any references to itself in
  // `tagToWs`, and even removes the hashmap entry if there are no more entries in the bucket.
  {
    auto managers = getManagersByEventWebSocketId().lockExclusive();
    auto webSockets = webSocketsForEventHandler.lockExclusive();

    // Scan the whole registry: it holds non-owning pointers, so every entry still naming this
    // manager has to go, including any whose `webSockets` entry was already removed.
    managers->eraseAll([this](kj::StringPtr, RegisteredManager& registered) {
      if (registered.manager != this) return false;
      registered.threadId.assertCurrentThread();
      return true;
    });

    webSockets->clear();
  }

  allWs.clear();
  KJ_ASSERT(tagToWs.size() == 0, "tagToWs hashmap wasn't cleared.");
}

kj::Own<Worker::Actor::HibernationManager> LegacyHibernationManagerImpl::addRef() {
  return kj::addRef(*this);
}

void LegacyHibernationManagerImpl::acceptWebSocket(
    jsg::Ref<api::WebSocket> ws, kj::ArrayPtr<kj::String> tags) {
  // First, we create the HibernatableWebSocket and add it to the collection where it'll stay
  // until it's destroyed.

  JSG_REQUIRE(allWs.size() < ACTIVE_CONNECTION_LIMIT, Error, "only ", ACTIVE_CONNECTION_LIMIT,
      " websockets can be accepted on a single Durable Object instance");

  auto hib = kj::heap<HibernatableWebSocket>(kj::mv(ws), tags, *this);
  HibernatableWebSocket& refToHibernatable = *hib.get();

  // TODO(mar): Improve accept span context capturing — route snapshotted user span context
  // to serialization point instead of capturing only the invocation root span here.
  auto invCtx = IoContext::current().getInvocationSpanContext();
  refToHibernatable.userSpanContext = tracing::SpanContext(invCtx.getTraceId(), invCtx.getSpanId());

  allWs.push_front(kj::mv(hib));
  refToHibernatable.node = allWs.begin();

  // If the `tags` array is empty (i.e. user did not provide a tag), we skip the population of the
  // `tagToWs` HashMap below and go straight to initiating the readLoop.

  // It is the caller's responsibility to ensure all elements of `tags` are unique.
  // TODO(cleanup): Maybe we could enforce uniqueness by using an immutable type that
  // can only be constructed if the elements in the collection are distinct, ex. "DistinctArray".
  //
  // We need to add the HibernatableWebSocket to each bucket in `tagToWs` corresponding to its tags.
  //  1. Create the entry if it doesn't exist
  //  2. Fill the TagListItem in the HibernatableWebSocket's tagItems array
  size_t position = 0;
  for (auto tag = tags.begin(); tag < tags.end(); tag++, position++) {
    auto& tagCollection = tagToWs.findOrCreate(*tag, [&tag]() {
      auto item = kj::heap<TagCollection>(
          kj::mv(*tag), kj::heap<kj::List<TagListItem, &TagListItem::link>>());
      return decltype(tagToWs)::Entry{item->tag, kj::mv(item)};
    });
    // This TagListItem sits in the HibernatableWebSocket's tagItems array.
    auto& tagListItem = refToHibernatable.tagItems[position];
    tagListItem.hibWS = refToHibernatable;
    tagListItem.tag = tagCollection->tag.asPtr();

    auto& list = tagCollection->list;
    list->add(tagListItem);
    // We also give the TagListItem a reference to the list it was added to so the
    // HibernatableWebSocket can quickly remove itself from the list without doing a lookup
    // in `tagToWs`.
    tagListItem.list = *list.get();
  }

  // Before starting the readLoop, we need to move the kj::Own<kj::WebSocket> from the
  // api::WebSocket into the HibernatableWebSocket and accept the api::WebSocket as "hibernatable".
  refToHibernatable.ws =
      refToHibernatable.activeOrPackage.get<jsg::Ref<api::WebSocket>>()->acceptAsHibernatable(
          refToHibernatable.getTags());

  // Finally, we initiate the readloop for this HibernatableWebSocket and
  // give the task to the HibernationManager so it lives long.
  readLoopTasks.add(handleReadLoop(refToHibernatable).catch_([](kj::Exception&& e) {
    if (isInterestingException(e)) {
      LOG_EXCEPTION_IF_INTERNAL("LegacyHibernationManagerImpl::handleReadLoop", e);
    }
  }));
}

kj::Promise<void> LegacyHibernationManagerImpl::handleReadLoop(
    HibernatableWebSocket& refToHibernatable) {
  kj::Maybe<kj::Exception> maybeException;
  try {
    co_await readLoop(refToHibernatable);
  } catch (...) {
    maybeException = kj::getCaughtExceptionAsKj();
  }
  co_await handleSocketTermination(refToHibernatable, maybeException);
}

kj::Vector<jsg::Ref<api::WebSocket>> LegacyHibernationManagerImpl::getWebSockets(
    jsg::Lock& js, kj::Maybe<kj::StringPtr> maybeTag) {
  kj::Vector<jsg::Ref<api::WebSocket>> matches;
  KJ_IF_SOME(tag, maybeTag) {
    KJ_IF_SOME(item, tagToWs.find(tag)) {
      auto& list = *((item)->list);
      for (auto& entry: list) {
        auto& hibWS = KJ_REQUIRE_NONNULL(entry.hibWS);
        matches.add(hibWS.getActiveOrUnhibernate(js));
      }
    }
  } else {
    // Add all websockets!
    for (auto& hibWS: allWs) {
      matches.add(hibWS->getActiveOrUnhibernate(js));
    }
  }
  return kj::mv(matches);
}

void LegacyHibernationManagerImpl::setWebSocketAutoResponse(
    kj::Maybe<kj::StringPtr> request, kj::Maybe<kj::StringPtr> response) {
  KJ_IF_SOME(req, request) {
    // If we have a request, we must also have a response. If response is kj::none, we'll throw.
    autoResponsePair->request = kj::str(req);
    autoResponsePair->response = kj::str(KJ_REQUIRE_NONNULL(response));
    return;
  }
  // If we don't have a request, we must unset both request and response.
  autoResponsePair->request = kj::none;
  autoResponsePair->response = kj::none;
}

kj::Maybe<jsg::Ref<api::WebSocketRequestResponsePair>> LegacyHibernationManagerImpl::
    getWebSocketAutoResponse(jsg::Lock& js) {
  KJ_IF_SOME(req, autoResponsePair->request) {
    // When getting the currently set auto-response pair, if we have a request we must have a response
    // set. If not, we'll throw.
    return api::WebSocketRequestResponsePair::constructor(
        js, kj::str(req), kj::str(KJ_REQUIRE_NONNULL(autoResponsePair->response)));
  }
  return kj::none;
}

void LegacyHibernationManagerImpl::setTimerChannel(TimerChannel& timerChannel) {
  timer = timerChannel;
}

void LegacyHibernationManagerImpl::hibernateWebSockets(Worker::Lock& lock) {
  JSG_WITHIN_CONTEXT_SCOPE(lock, lock.getContext(), [&](jsg::Lock& js) {
    for (auto& ws: allWs) {
      KJ_IF_SOME(active, ws->activeOrPackage.tryGet<jsg::Ref<api::WebSocket>>()) {
        // Transfers ownership of properties from api::WebSocket to HibernatableWebSocket via the
        // HibernationPackage.
        ws->activeOrPackage.init<api::WebSocket::HibernationPackage>(
            active.get()->buildPackageForHibernation());
      } else {
      }  // Here to quash compiler warning
    }
  });
}

void LegacyHibernationManagerImpl::setEventTimeout(kj::Maybe<uint32_t> timeoutMs) {
  eventTimeoutMs = timeoutMs;
}

kj::Maybe<uint32_t> LegacyHibernationManagerImpl::getEventTimeout() {
  return eventTimeoutMs;
}

kj::Maybe<kj::Own<Worker::Actor::HibernationManager>> LegacyHibernationManagerImpl::
    findManagerForEvent(kj::StringPtr websocketId) {
  // A shared lock is enough: this only reads the map, and holding it still excludes the erasers, so
  // a matching entry's manager stays alive across the addRef() below.
  auto managers = getManagersByEventWebSocketId().lockShared();
  KJ_IF_SOME(manager, managers->find(websocketId)) {
    if (manager.threadId != kj::ThreadId::current()) return kj::none;
    return manager.manager->addRef();
  }
  return kj::none;
}

LegacyHibernationManagerImpl::HibernatableWebSocket& LegacyHibernationManagerImpl::
    takeWebSocketForEvent(kj::StringPtr websocketId) {
  auto& actor = KJ_REQUIRE_NONNULL(IoContext::current().getActor());
  KJ_IF_SOME(manager, actor.getHibernationManager()) {
    auto& legacyManager = kj::downcast<LegacyHibernationManagerImpl>(manager);
    KJ_IF_SOME(hibernatableWebSocket, legacyManager.tryTakeWebSocketForEventHandler(websocketId)) {
      return hibernatableWebSocket;
    }
  }

  KJ_IF_SOME(hibernatableWebSocket, tryTakeWebSocketForGlobalEvent(websocketId)) {
    return hibernatableWebSocket;
  }

  KJ_FAIL_REQUIRE("hibernatable WebSocket event missing instance map entry", websocketId);
}

kj::Maybe<LegacyHibernationManagerImpl::HibernatableWebSocket&> LegacyHibernationManagerImpl::
    tryTakeWebSocketForGlobalEvent(kj::StringPtr websocketId) {
  auto managers = getManagersByEventWebSocketId().lockExclusive();
  KJ_IF_SOME(entry, managers->findEntry(websocketId)) {
    if (entry.value.threadId != kj::ThreadId::current()) return kj::none;

    // Erase on every exit path below. An entry whose instance-map counterpart is gone or
    // inconsistent is stale, and stranding it here would keep it in the registry until the manager
    // is destroyed -- cancelEvent() relies on a missing instance entry meaning the registry is
    // clear too.
    KJ_DEFER(managers->erase(entry));

    auto taken =
        KJ_REQUIRE_NONNULL(entry.value.manager->tryTakeWebSocketForEventHandlerImpl(websocketId),
            "hibernatable WebSocket event missing instance map entry", websocketId);
    KJ_ASSERT(taken.registeredGlobally,
        "hibernatable WebSocket event manager registry pointed to unregistered instance entry",
        websocketId);
    return *taken.webSocket;
  }

  return kj::none;
}

void LegacyHibernationManagerImpl::putWebSocketForEventHandler(
    kj::String websocketId, HibernatableWebSocket& hib) {
  auto websocketIdPtr = websocketId.asPtr();
  auto webSockets = webSocketsForEventHandler.lockExclusive();
  KJ_ASSERT(webSockets->find(websocketIdPtr) == kj::none,
      "duplicate hibernatable WebSocket event ID", websocketIdPtr);
  webSockets->insert(kj::mv(websocketId), EventWebSocketEntry{&hib});
}

void LegacyHibernationManagerImpl::registerManagerForEvent(kj::StringPtr websocketId) {
  auto managers = getManagersByEventWebSocketId().lockExclusive();
  auto webSockets = webSocketsForEventHandler.lockExclusive();
  auto& entry = KJ_REQUIRE_NONNULL(webSockets->findEntry(websocketId),
      "hibernatable WebSocket event missing instance map entry", websocketId);
  if (entry.value.registeredGlobally) {
    auto& manager = KJ_REQUIRE_NONNULL(managers->find(websocketId),
        "hibernatable WebSocket event missing manager registry entry", websocketId);
    manager.threadId.assertCurrentThread();
    KJ_ASSERT(manager.manager == this,
        "hibernatable WebSocket event registered to a different manager", websocketId);
    return;
  }

  KJ_ASSERT(managers->find(websocketId) == kj::none,
      "duplicate hibernatable WebSocket event manager", websocketId);
  managers->insert(kj::str(websocketId), RegisteredManager{kj::ThreadId::current(), this});
  entry.value.registeredGlobally = true;
}

void LegacyHibernationManagerImpl::cancelEvent(kj::StringPtr websocketId) {
  {
    // Only RPC dispatch and cross-manager wakes reach the process-global registry, so most events
    // can be cleaned up under this manager's own lock. A missing instance entry means the event was
    // already claimed, and the claim paths clear the registry alongside it.
    auto webSockets = webSocketsForEventHandler.lockExclusive();
    KJ_IF_SOME(entry, webSockets->findEntry(websocketId)) {
      if (!entry.value.registeredGlobally) {
        webSockets->erase(entry);
        return;
      }
    } else {
      return;
    }
  }

  auto managers = getManagersByEventWebSocketId().lockExclusive();
  auto webSockets = webSocketsForEventHandler.lockExclusive();
  KJ_IF_SOME(manager, managers->findEntry(websocketId)) {
    manager.value.threadId.assertCurrentThread();
    KJ_ASSERT(manager.value.manager == this,
        "hibernatable WebSocket event registered to a different manager", websocketId);
    managers->erase(manager);
  }
  KJ_IF_SOME(entry, webSockets->findEntry(websocketId)) {
    webSockets->erase(entry);
  }
}

kj::Maybe<LegacyHibernationManagerImpl::HibernatableWebSocket&> LegacyHibernationManagerImpl::
    tryTakeWebSocketForEventHandler(kj::StringPtr websocketId) {
  {
    auto webSockets = webSocketsForEventHandler.lockExclusive();
    KJ_IF_SOME(entry, webSockets->findEntry(websocketId)) {
      if (!entry.value.registeredGlobally) {
        auto& webSocket = *entry.value.webSocket;
        webSockets->erase(websocketId);
        return webSocket;
      }
    } else {
      return kj::none;
    }
  }

  auto managers = getManagersByEventWebSocketId().lockExclusive();
  auto webSockets = webSocketsForEventHandler.lockExclusive();
  KJ_IF_SOME(entry, webSockets->findEntry(websocketId)) {
    KJ_ASSERT(entry.value.registeredGlobally,
        "hibernatable WebSocket event manager registry pointed to unregistered instance entry",
        websocketId);

    KJ_IF_SOME(registeredEntry, managers->findEntry(websocketId)) {
      registeredEntry.value.threadId.assertCurrentThread();
      KJ_ASSERT(registeredEntry.value.manager == this,
          "hibernatable WebSocket event registered to a different manager", websocketId);

      auto& webSocket = *entry.value.webSocket;
      webSockets->erase(websocketId);
      managers->erase(registeredEntry);
      return webSocket;
    }
  }

  return kj::none;
}

kj::Maybe<LegacyHibernationManagerImpl::TakenEventWebSocket> LegacyHibernationManagerImpl::
    tryTakeWebSocketForEventHandlerImpl(kj::StringPtr websocketId) {
  auto webSockets = webSocketsForEventHandler.lockExclusive();
  KJ_IF_SOME(entry, webSockets->findEntry(websocketId)) {
    auto& webSocket = *entry.value.webSocket;
    auto registeredGlobally = entry.value.registeredGlobally;

    webSockets->erase(websocketId);
    return TakenEventWebSocket{&webSocket, registeredGlobally};
  }

  return kj::none;
}

void LegacyHibernationManagerImpl::dropHibernatableWebSocket(HibernatableWebSocket& hib) {
  removeFromAllWs(hib);
}

inline void LegacyHibernationManagerImpl::removeFromAllWs(HibernatableWebSocket& hib) {
  auto& node = KJ_REQUIRE_NONNULL(hib.node);
  allWs.erase(node);
}

kj::Promise<void> LegacyHibernationManagerImpl::handleSocketTermination(
    HibernatableWebSocket& hib, kj::Maybe<kj::Exception>& maybeError) {
  // A failed termination event must not leave a disconnected socket in either registry.
  kj::String eventWebSocketId;
  KJ_DEFER({
    if (eventWebSocketId.size() > 0) {
      cancelEvent(eventWebSocketId);
    }
    dropHibernatableWebSocket(hib);
  });

  kj::Maybe<kj::Promise<void>> event;
  KJ_IF_SOME(error, maybeError) {
    auto websocketId = randomUUID(kj::none);
    eventWebSocketId = kj::str(websocketId);
    putWebSocketForEventHandler(kj::str(websocketId), hib);
    kj::Maybe<api::HibernatableSocketParams> params;
    if (!hib.hasDispatchedClose && (error.getType() == kj::Exception::Type::DISCONNECTED)) {
      // If premature disconnect/cancel, dispatch a close event if we haven't already.
      hib.hasDispatchedClose = true;
      params = api::HibernatableSocketParams(1006,
          kj::str("WebSocket disconnected without sending Close frame."), false,
          kj::mv(websocketId));
    } else {
      // Otherwise, we need to dispatch an error event!
      params = api::HibernatableSocketParams(kj::mv(error), kj::mv(websocketId));
    }

    KJ_REQUIRE_NONNULL(params).setTimeout(eventTimeoutMs);
    auto workerInterface = getWorkerForEvent(hib);
    event = workerInterface
                ->customEvent(kj::rc<api::HibernatableWebSocketCustomEvent>(
                    hibernationEventType, kj::mv(KJ_REQUIRE_NONNULL(params)), *this)
                                  .toOwn())
                .ignoreResult()
                .attach(kj::mv(workerInterface));
  }

  // Returning the event promise will store it in readLoopTasks.
  // After the task completes, we want to drop the websocket since we've closed the connection.
  KJ_IF_SOME(promise, event) {
    co_await promise;
  }
}

kj::Own<WorkerInterface> LegacyHibernationManagerImpl::getWorkerForEvent(
    HibernatableWebSocket& hib) {
  SpanParent userSpanParent = SpanParent(nullptr);
  KJ_IF_SOME(ctx, hib.userSpanContext) {
    userSpanParent = SpanParent::fromSpanContext(tracing::SpanContext::clone(ctx));
  }
  return loopback->getWorker({
    .userSpanParent = kj::mv(userSpanParent),
    .forceFreshActorCode = hib.activeOrPackage.is<api::WebSocket::HibernationPackage>()
        ? ForceFreshActorCode::YES
        : ForceFreshActorCode::NO,
  });
}

kj::Promise<void> LegacyHibernationManagerImpl::readLoop(HibernatableWebSocket& hib) {
  // Like the api::WebSocket readLoop(), but we dispatch different types of events.
  auto& ws = *KJ_REQUIRE_NONNULL(hib.ws);
  while (true) {
    kj::WebSocket::Message message = co_await ws.receive();
    // Note that errors are handled by the callee of `readLoop`, since we throw from `receive()`.

    auto skip = false;

    // If we have a request != kj::none, we can compare it the received message. This also implies
    // that we have a response set in autoResponsePair.
    KJ_IF_SOME(req, autoResponsePair->request) {
      KJ_SWITCH_ONEOF(message) {
        KJ_CASE_ONEOF(text, kj::String) {
          if (text == req) {
            // If the received message matches the one set for auto-response, we must
            // short-circuit readLoop, store the current timestamp and and automatically respond
            // with the expected response.
            TimerChannel& timerChannel = KJ_REQUIRE_NONNULL(timer);
            // This should count as a new IO event, hence we should call syncTime
            // otherwise the autoResponseTimestamp wouldn't be accurate.
            timerChannel.syncTime();
            // We should have set the timerChannel previously in the hibernation manager.
            // If we haven't, we aren't able to get the current time.
            hib.autoResponseTimestamp = timerChannel.now();
            // We'll store the current timestamp in the HibernatableWebSocket to assure it gets
            // stored even if the WebSocket is currently hibernating. In that scenario, the timestamp
            // value will be loaded into the WebSocket during unhibernation.
            // Copy autoResponsePair->response into a coroutine-local kj::String before either
            // branch sends it. The hibernated branch's ws.send() borrows the underlying
            // ArrayPtr across the co_await per kj::WebSocket::send()'s documented contract,
            // and any concurrent JS call to state.setWebSocketAutoResponse() would reassign
            // or clear autoResponsePair->response, freeing the buffer while the write is
            // still in flight. The active branch's sendAutoResponse takes ownership of the
            // kj::String anyway, so hoisting the copy serves both cases with a single
            // allocation.
            auto responseCopy = kj::str(KJ_REQUIRE_NONNULL(autoResponsePair->response));
            KJ_SWITCH_ONEOF(hib.activeOrPackage) {
              KJ_CASE_ONEOF(apiWs, jsg::Ref<api::WebSocket>) {
                // If the actor is not hibernated/If the WebSocket is active, we need to update
                // autoResponseTimestamp on the active websocket.
                apiWs->setAutoResponseStatus(hib.autoResponseTimestamp, kj::READY_NOW);
                // Since we had a request set, we must have and response that's sent back using the
                // same websocket here. The sending of response is managed in web-socket to avoid
                // possible racing problems with regular websocket messages.
                co_await apiWs->sendAutoResponse(kj::mv(responseCopy), ws);
              }
              KJ_CASE_ONEOF(package, api::WebSocket::HibernationPackage) {
                if (!package.closedOutgoingConnection) {
                  // We need to store the autoResponsePromise because we may instantiate an api::websocket
                  // If we do that, we have to provide it with the promise to avoid races. This can
                  // happen if we have a websocket hibernating, that unhibernates and sends a
                  // message while ws.send() for auto-response is also sending.
                  auto p = ws.send(responseCopy.asArray()).fork();
                  hib.autoResponsePromise = p.addBranch();
                  co_await p;
                  hib.autoResponsePromise = kj::READY_NOW;
                }
              }
            }
            // If we've sent an auto response message, we should not unhibernate or deliver the
            // received message to the actor
            skip = true;
          }
        }
        KJ_CASE_ONEOF_DEFAULT {}
      }
    }

    if (skip) {
      continue;
    }

    auto websocketId = randomUUID(kj::none);
    auto eventWebSocketId = kj::str(websocketId);
    putWebSocketForEventHandler(kj::str(websocketId), hib);
    KJ_DEFER(cancelEvent(eventWebSocketId));

    // Build the event params depending on what type of message we got.
    kj::Maybe<api::HibernatableSocketParams> maybeParams;
    KJ_SWITCH_ONEOF(message) {
      KJ_CASE_ONEOF(text, kj::String) {
        maybeParams.emplace(kj::mv(text), kj::mv(websocketId));
      }
      KJ_CASE_ONEOF(data, kj::Array<kj::byte>) {
        maybeParams.emplace(kj::mv(data), kj::mv(websocketId));
      }
      KJ_CASE_ONEOF(close, kj::WebSocket::Close) {
        maybeParams.emplace(close.code, kj::mv(close.reason), true, kj::mv(websocketId));
        // We'll dispatch the close event, so let's mark our websocket as having done so to
        // prevent a situation where we dispatch it twice.
        hib.hasDispatchedClose = true;
      }
    }

    auto params = kj::mv(KJ_REQUIRE_NONNULL(maybeParams));
    params.setTimeout(eventTimeoutMs);
    auto isClose = params.isCloseEvent();
    auto workerInterface = getWorkerForEvent(hib);
    co_await workerInterface->customEvent(
        kj::rc<api::HibernatableWebSocketCustomEvent>(hibernationEventType, kj::mv(params), *this)
            .toOwn());
    if (isClose) {
      co_return;
    }
  }
}

};  // namespace workerd
