#pragma once
#include <cstdint>

enum class RA_ReaderMode { Blocking, Keyspace };

enum class RA_ReaderFallbackReason {
  None, Starting, UnverifiedNotifications, NotificationsDisabled,
  SubscriptionRejected, SubscriberUnavailable, MissedNotifications
};

struct RA_ReaderBackendStatus {
  RA_ReaderMode requested = RA_ReaderMode::Blocking;
  RA_ReaderMode effective = RA_ReaderMode::Blocking;
  RA_ReaderFallbackReason fallbackReason = RA_ReaderFallbackReason::None;
};
