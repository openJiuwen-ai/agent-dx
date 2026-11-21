// Linux placement preference for the ACL owner thread's pinned host allocations.
#pragma once

#include <cstdint>

namespace afs_hixl_numa {
struct HostNode {
    int id;
    const char *source;
};

// Called on the allocating ACL owner thread. An unknown node has id -1.
HostNode preferred_node(int32_t user_device);

class ScopedPreference {
public:
    explicit ScopedPreference(int node);
    ~ScopedPreference();
    ScopedPreference(const ScopedPreference &) = delete;
    ScopedPreference &operator=(const ScopedPreference &) = delete;
    bool applied() const { return applied_; }
    bool restore();
private:
    bool applied_ = false;
};
}  // namespace afs_hixl_numa
