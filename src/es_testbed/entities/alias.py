"""Alias Entity Class"""

# pylint: disable=C0115,C0116,R0902,R0904,R0913,R0917
import typing as t
import logging
from ..debug import debug, begin_end
from ..es_api import resolver, rollover
from ..utils import prettystr
from .entity import Entity

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)


class Alias(Entity):

    def __init__(
        self,
        client: "Elasticsearch",
        name: t.Union[str, None] = None,
    ):
        debug.lv2("Initializing Alias entity object...")
        super().__init__(client=client, name=name)
        debug.lv3("Alias entity object initialized")

    @begin_end()
    def rollover(self) -> None:
        """Rollover the alias"""
        rollover(self.client, self.name)

    @begin_end()
    def verify(self, index_list: t.Sequence[str]) -> bool:
        retval = False
        res = resolver(self.client, self.name)
        debug.lv5(f"resolver response: {prettystr(res)}")
        for idx, alias in enumerate(res["aliases"]):
            if alias["name"] == self.name:
                debug.lv3(
                    f"Confirm list index position [{idx}] match of alias "
                    f"{prettystr(alias)}"
                )
            else:
                continue
            if alias["indices"] == index_list:
                debug.lv3(f"Confirm match of indices backed by alias {self.name}")
                retval = True
                break
        debug.lv5(f"Return value = {retval}")
        return retval
