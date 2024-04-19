"""Args Parent Class"""
import typing as t
import logging
from es_testbed.defaults import ARGSCLASSES

class Args(t.Dict):
    """
    Initialize with None values for all accepted settings

    Contains :py:meth:`update_settings` and :py:meth:`asdict` methods
    """
    def __init__(
            self,
            settings: t.Dict[str, t.Any] = None,
            defaults: t.Dict[str, t.Any] = None,
        ):
        """Updatable object that will contain arguments for connecting to Elasticsearch"""
        self.logger = logging.getLogger(__name__)
        self.settings = settings
        if defaults is None:
            defaults = {}
        self.set_defaults(defaults)
        if settings is None:
            self.settings = defaults
        else:
            # Only do this if we sent actual settings
            self.update_settings(self.settings)

    def set_defaults(self, defaults: dict) -> None:
        """Set attr values from defaults"""
        for key, value in defaults.items():
            setattr(self, key, value)
            # Override self.settings if no default for key is found
            if key not in self.settings:
                self.settings[key] = value

    def update_settings(self, new_settings: dict) -> None:
        """Update individual settings from provided new_settings dict"""
        for key, value in new_settings.items():
            setattr(self, key, value)

    def _object_class(self, val: t.Any) -> str:
        """
        Where type(val) = <class 'base.secondary.tertiary.ClassName'>
        Extract 'ClassName' from type(val)
        """
        return str(type(val)).split("'")[1].split('.')[-1]

    def _expand_args(self, setting: str) -> t.Union[str, t.Dict[str, str]]:
        val = getattr(self, setting, None)
        if self._object_class(val) in ARGSCLASSES: # These have a .asdict like this
            return val.asdict
        return val

    @property
    def asdict(self) -> dict:
        """Return as a dictionary"""
        retval = {}
        if isinstance(self.settings, dict):
            for setting in self.settings:
                retval[setting] = self._expand_args(setting)
        return retval
