# Security Overview: Privileged Outer Container with Rootless Podman and SELinux

## Environment Summary

| Component | Configuration |
|-----------|---------------|
| **Host** | RHEL with SELinux in enforcing mode |
| **Podman daemon** | Running rootless as user `omero` (non-root) |
| **Outer container** | Started with `--privileged` but running as non-root user (`omero` or similar) |
| **Inner containers** | Run as root inside the outer container, managed by Podman |

## Key Security Considerations

### 1. Rootless Podman on Host Limits Privileges

- Podman runs **without host root privileges**
- Uses user namespaces and SELinux to isolate containers from the host OS
- Prevents containers from gaining elevated access to the host system
- Creates a security boundary between containerized processes and host

### 2. Privileged Outer Container — Context Matters

- While `--privileged` grants broad Linux capabilities inside the container, your outer container runs as a **non-root user**
- This limits the ability to exploit these capabilities on the host
- The privileged flag operates within the container's user namespace, not the host's

### 3. SELinux Enforcing Mode Provides Strong Containment

- SELinux policies restrict container access to host resources and devices
- Provides an additional security layer protecting the host system
- **Even privileged containers** are subject to SELinux policy enforcement
- Creates mandatory access controls beyond traditional Unix permissions

### 4. Inner Containers Running as Root Are Confined

- Root inside the inner container corresponds **only to root within the container's user namespace and mount namespace**
- The inner container cannot escape to host root due to:
  - Outer container user restrictions
  - User namespaces
  - SELinux policies
- Container root ≠ Host root

### 5. File and Device Access Are Scoped

- Mounted volumes and devices (e.g., `/dev/fuse`) are explicitly managed and scoped by Podman
- The outer container setup prevents arbitrary host device access
- Volume mounts are mediated through the container runtime security model
## Security Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                        RHEL Host                            │
│                     SELinux: Enforcing                      │
├─────────────────────────────────────────────────────────────┤
│   Rootless Podman (user: omero)                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │            Outer Container                            │  │
│  │         (--privileged, user: autoimportuser)          │  │
│  │                                                       │  │
│  │  ┌─────────────────┐   ┌─────────────────┐            │  │
│  │  │ Inner Container │   │ Inner Container │            │  │
│  │  │  (root inside)  │   │  (root inside)  │            │  │
│  │  │                 │   │                 │            │  │
│  │  │  convertleica   │   │ cimagexpress... │            │  │
│  │  └─────────────────┘   └─────────────────┘            │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```


## Summary

✅ **The privileged flag in your outer container increases container capabilities but does not grant host root access** due to:
- Running rootless Podman as non-root user
- SELinux enforcement
- User namespace isolation

✅ **This setup balances the need for container functionality** (e.g., user namespace mapping, fuse support) with strong host protection.

✅ **Permission and security risks from running inner containers as root are contained** within the container boundary and do not translate to host-level privileges.

## Risk Assessment

| Risk Level | Component | Mitigation |
|------------|-----------|------------|
| 🟢 **Low** | Host system compromise | Rootless Podman + SELinux + User namespaces |
| 🟡 **Medium** | Container escape | SELinux policies + Non-root outer container |
| 🟢 **Low** | Data access | Scoped volume mounts + File permissions |
| 🟢 **Low** | Device access | Controlled device mapping + SELinux |

## Best Practices Implemented

- ✅ **Defense in depth**: Multiple security layers (SELinux, user namespaces, rootless Podman)
- ✅ **Principle of least privilege**: Non-root outer container user
- ✅ **Container isolation**: Proper namespace separation
- ✅ **Access control**: SELinux mandatory access controls
- ✅ **Scoped permissions**: Limited device and file system access

---

*This security model provides robust isolation while maintaining the flexibility needed for containerized data processing