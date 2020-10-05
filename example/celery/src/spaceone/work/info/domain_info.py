from spaceone.api.report.v1 import domain_pb2
from spaceone.core.pygrpc.message_type import *

__all__ = ['DomainInfo', 'DomainsInfo']


def IdentityInfo(vo):
    info = {
        "key": vo.key,
        "value": vo.value,
    }
    return domain_pb2.IdentityInfo(**info)


def RelationResourceInfo(vo):
    info = {
        "service": vo.service,
        "resource": vo.resource,
        "identity": list(map(IdentityInfo, vo.identity)),
        "tags": change_struct_type(vo.tags),
    }
    return domain_pb2.RelationResourceInfo(**info)


def DomainInfo(vo):
    info = {
        "domain_id": vo.domain_id,
        "register_templates": vo.register_templates,
        "relation_resources": list(map(RelationResourceInfo, vo.relation_resources)),
    }
    return domain_pb2.DomainInfo(**info)


def DomainsInfo(results, total_count):
    info = {
        "results": list(map(DomainInfo, results)),
        "total_count": total_count,
    }
    return domain_pb2.DomainsInfo(**info)
