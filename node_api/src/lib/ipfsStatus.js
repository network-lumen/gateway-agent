let lastIpfsCheckAt = 0;
let lastIpfsOnline = false;

export function setIpfsStatus(online, timestampMs) {
  const ts =
    typeof timestampMs === 'number' && Number.isFinite(timestampMs)
      ? timestampMs
      : Date.now();
  lastIpfsOnline = !!online;
  lastIpfsCheckAt = ts;
}

export function getIpfsStatusSnapshot() {
  return {
    online: !!lastIpfsOnline,
    lastCheckAt: lastIpfsCheckAt || null
  };
}

