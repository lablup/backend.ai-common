import re
from typing import (
    Sequence, List, Set, Any
)

from .types import ServicePort, ServicePortProtocols

__all__ = (
    'parse_service_ports'
)

_rx_service_ports = re.compile(
    r'^(?P<name>[\w-]+):(?P<proto>\w+):(?P<ports>\[\d+(?:,\d+)*\]|\d+)(?:,|$)')


def parse_service_ports(s: str, exception: Any) -> Sequence[ServicePort]:
    items: List[ServicePort] = []
    used_ports: Set[int] = set()
    while True:
        match = _rx_service_ports.search(s)
        if match:
            s = s[len(match.group(0)):]
            name = match.group('name')
            if not name:
                raise exception('Service port name must be not empty.')
            protocol = match.group('proto')
            if protocol == 'pty':
                # unsupported, skip
                continue
            if protocol not in ('tcp', 'http', 'preopen'):
                raise exception(f'Unsupported service port protocol: {protocol}')
            ports = tuple(map(int, match.group('ports').strip('[]').split(',')))
            for p in ports:
                if p in used_ports:
                    raise exception(f'The port {p} is already used by another service port.')
                if p >= 65535:
                    raise exception(f'The service port number {p} must be smaller than 65535.')
                if p in (2000, 2001, 2002, 2003, 2200, 7681):
                    raise exception('The service ports 2000 to 2003, 2200 and 7681 '
                                       'are reserved for internal use.')
                used_ports.add(p)
            items.append({
                'name': name,
                'protocol': ServicePortProtocols(protocol),
                'container_ports': ports,
                'host_ports': (None,) * len(ports),
            })
        else:
            break
        if not s:
            break
    return items
