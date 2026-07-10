# Security Policy

## Supported Versions

Security fixes are applied to the latest released `0.x` line. Older development
snapshots are not supported.

## Reporting a Vulnerability

Do not open a public issue for an unpatched vulnerability. Use GitHub private
vulnerability reporting from the repository's Security page. Include affected
versions, impact, reproduction steps, and any suggested mitigation. Repository
owners must enable private vulnerability reporting before announcing a public
release.

SectorSync exposes framing and security policy hooks but does not ship production
cryptographic algorithms, account authentication, anti-cheat, or secret storage.
Vulnerabilities in an embedding application's adapters should also be reported
to that application's owner.
