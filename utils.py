import os
import re
import subprocess

try:
    from shlex import quote as sh_quote
except ImportError:
    from pipes import quote as sh_quote


def ensure_config_var(obj, var):
    if var not in obj:
        raise RuntimeError('Missing mandatory option %s in config' % var)


def get_cpuinfo_serial():
    data = open('/proc/cpuinfo').read()
    matches = re.findall('^Serial\s+: ([0-9a-f]+)$', data, re.M)
    if len(matches) > 0:
        return matches[0]
    return None


def get_stored_serial():
    fname = '/var/lib/wirenboard/serial.conf'
    if os.path.exists(fname):
        return open(fname).read().strip()
    else:
        raise RuntimeError("cannot get serial")


def get_wb_version():
    return os.environ.get('WB_VERSION')

def get_iface_ip(iface):
    cmd = subprocess.Popen("/sbin/ifconfig %s | grep \"inet addr\" | awk -F: '{print $2}' | awk '{print $1}'" % sh_quote(iface), shell=True, stdout=subprocess.PIPE)
    ip = cmd.stdout.readline().strip()
    return ip or None
