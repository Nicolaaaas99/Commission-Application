"""Evolution SDK connection. Loads the Pastel.Evolution DLL via pythonnet and
ensures live company + common DB connections before SDK use.

The SDK's connection state is tracked statically inside .NET (and may be
thread-local under the hood), so we always re-check IsConnectionOpen before
returning the SDK. This handles both per-thread connection state (Waitress
worker threads) and stale connections."""
import sys
import os
import threading
import pythonnet
pythonnet.load("netfx")
import clr

import config

_lock = threading.Lock()
_dll_loaded = False
SDK = None


def _load_dll():
    global _dll_loaded, SDK
    if _dll_loaded:
        return
    dll_dir = os.path.dirname(config.DLL_PATH)
    if dll_dir not in sys.path:
        sys.path.insert(0, dll_dir)
    clr.AddReference(config.DLL_PATH)
    from Pastel.Evolution import DatabaseContext  # noqa: F401  ensure namespace is touched
    import Pastel.Evolution as _SDK
    SDK = _SDK
    _dll_loaded = True


def connect():
    """Ensure the SDK has live company + common DB connections. Idempotent.

    Safe to call before every SDK operation — it only does the heavy
    CreateConnection work if IsConnectionOpen reports false.
    """
    with _lock:
        _load_dll()
        from Pastel.Evolution import DatabaseContext

        try:
            already_open = DatabaseContext.IsConnectionOpen and DatabaseContext.IsCommonConnectionOpen
        except Exception:
            already_open = False

        if not already_open:
            DatabaseContext.CreateCommonDBConnection(
                config.SERVER, config.COMMON_DATABASE,
                config.USERNAME, config.PASSWORD, False)
            DatabaseContext.SetLicense(config.LICENSE_KEY, config.LICENSE_CODE)
            DatabaseContext.CreateConnection(
                config.SERVER, config.COMPANY_DATABASE,
                config.USERNAME, config.PASSWORD, False)
            print("Connected to Evolution successfully.")

        # Evolution requires an "agent" (operator) to own posted batches.
        # Always (re)set: CurrentAgent may be per-thread, and re-setting is cheap.
        agent_code = getattr(config, 'AGENT_CODE', None)
        if agent_code:
            from Pastel.Evolution import Agent
            DatabaseContext.CurrentAgent = Agent(str(agent_code))


def reconnect():
    """Close and reopen the SDK connections. Mirrors what an app process
    restart does, which Evolution treats as starting a new audit-number
    collection (so each batch gets its own '24583.NNNN' style prefix
    instead of all sharing one prefix). Use before each batch post."""
    with _lock:
        _load_dll()
        from Pastel.Evolution import DatabaseContext

        # Close existing connections so SQL Server sees a clean re-open.
        for attr in ('DBConnection', 'CommonDBConnection'):
            try:
                conn_obj = getattr(DatabaseContext, attr, None)
                if conn_obj is not None:
                    try:
                        conn_obj.Close()
                    except Exception:
                        pass
            except Exception:
                pass

        DatabaseContext.CreateCommonDBConnection(
            config.SERVER, config.COMMON_DATABASE,
            config.USERNAME, config.PASSWORD, False)
        DatabaseContext.SetLicense(config.LICENSE_KEY, config.LICENSE_CODE)
        DatabaseContext.CreateConnection(
            config.SERVER, config.COMPANY_DATABASE,
            config.USERNAME, config.PASSWORD, False)

        agent_code = getattr(config, 'AGENT_CODE', None)
        if agent_code:
            from Pastel.Evolution import Agent
            DatabaseContext.CurrentAgent = Agent(str(agent_code))


def get_sdk():
    connect()
    return SDK
