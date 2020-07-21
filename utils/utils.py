import os, sys, time, signal, atexit, subprocess
import socket
import ConfigParser

from kubernetes import client, config
from kubernetes.client.rest import ApiException

class parser(ConfigParser.ConfigParser):  
    def __init__(self,defaults=None):  
        ConfigParser.ConfigParser.__init__(self,defaults=None)  
    def optionxform(self, optionstr):  
        return optionstr

def runCmdRaiseException(cmd, head='VirtctlError', use_read=False, timeout=10):
    std_err = None
    if not cmd:
        return
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        t_beginning = time.time() 
        seconds_passed = 0 
        while True: 
            if p.poll() is not None: 
                break 
            seconds_passed = time.time() - t_beginning 
            if timeout and seconds_passed > timeout: 
                p.kill()
                raise TimeoutError(cmd, timeout) 
            time.sleep(0.1) 
        if use_read:
            std_out = p.stdout.read()
            std_err = p.stderr.read()
        else:
            std_out = p.stdout.readlines()
            std_err = p.stderr.readlines()
        if std_err:
            raise ExecuteException(head, std_err)
        return std_out
    finally:
        p.stdout.close()
        p.stderr.close()
        
class ExecuteException(Exception):
    def __init__(self, reason, message):
        self.reason = reason
        self.message = message 
        
class TimeoutError(Exception): 
    pass 

def get_field_in_kubernetes_by_index(name, group, version, plural, index):
    try:
        if not index or not list(index):
            return None
        jsondict = client.CustomObjectsApi().get_namespaced_custom_object(
        group=group, version=version, namespace='default', plural=plural, name=name)
        return get_field(jsondict, index)
    except:
        return None
    
def get_hostname_in_lower_case():
    cfg = "/etc/kubevmm/config"
    if not os.path.exists(cfg):
        cfg = "/home/kubevmm/bin/config"
    config_raw = parser()
    config_raw.read(cfg)
    prefix = config_raw.get('Kubernetes', 'hostname_prefix')
    if prefix == 'vm':
        return 'vm.%s' % socket.gethostname().lower()
    else:
        return socket.gethostname().lower()
    
def get_field(jsondict, index):
    retv = None
    '''
    Iterate keys in 'spec' structure and map them to real CMDs in back-end.
    Note that only the first CMD will be executed.
    '''
    contents = jsondict
    for layer in index[:-1]:
#         print(contents)
        contents = contents.get(layer)
    if not contents:
        return None
    for k, v in contents.items():
        if k == index[-1]:
            retv = v
    return retv

def get_field_in_kubernetes_node(name, index):
    try:
        v1_node_list = client.CoreV1Api().list_node(label_selector='host=%s' % name)
        jsondict = v1_node_list.to_dict()
        items = jsondict.get('items')
        if items:
            return get_field(items[0], index)
        else:
            return None
    except:
        return None

def list_all_disks(path, disk_type = 'f'):
    try:
        return runCmdRaiseException("timeout 10 find %s -type %s ! -name '*.json' ! -name '*.temp' ! -name 'content' ! -name '.*' ! -name '*.xml' ! -name '*.pem' | grep -v overlay2" % (path, disk_type))
    except:
        return []

class CDaemon:
    '''
    a generic daemon class.
    usage: subclass the CDaemon class and override the run() method
    stderr:
    verbose:
    save_path:
    '''
    def __init__(self, save_path, stdin=os.devnull, stdout=os.devnull, stderr=os.devnull, home_dir='.', umask=022, verbose=1):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = save_path
        self.home_dir = home_dir
        self.verbose = verbose
        self.umask = umask
        self.daemon_alive = True
 
    def daemonize(self):
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.stderr.write('fork #1 failed: %d (%s)\n' % (e.errno, e.strerror))
            sys.exit(1)
 
        os.chdir(self.home_dir)
        os.setsid()
        os.umask(self.umask)
 
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.stderr.write('fork #2 failed: %d (%s)\n' % (e.errno, e.strerror))
            sys.exit(1)
 
        sys.stdout.flush()
        sys.stderr.flush()
 
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        if self.stderr:
            se = file(self.stderr, 'a+', 0)
        else:
            se = so
 
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
 
        def sig_handler(signum, frame):
            self.daemon_alive = False
        signal.signal(signal.SIGTERM, sig_handler)
        signal.signal(signal.SIGINT, sig_handler)
 
        if self.verbose >= 1:
            print 'daemon process started ...'
 
        atexit.register(self.del_pid)
        pid = str(os.getpid())
        file(self.pidfile, 'w+').write('%s\n' % pid)
 
    def get_pid(self):
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
        except SystemExit:
            pid = None
        return pid
 
    def del_pid(self):
        if os.path.exists(self.pidfile):
            os.remove(self.pidfile)
 
    def start(self, *args, **kwargs):
        if self.verbose >= 1:
            print 'ready to starting ......'
        #check for a pid file to see if the daemon already runs
        pid = self.get_pid()
        if pid:
            msg = 'pid file %s already exists, is it already running?\n'
            sys.stderr.write(msg % self.pidfile)
            sys.exit(1)
        #start the daemon
        self.daemonize()
        self.run(*args, **kwargs)
 
    def stop(self):
        if self.verbose >= 1:
            print 'stopping ...'
        pid = self.get_pid()
        if not pid:
            msg = 'pid file [%s] does not exist. Not running?\n' % self.pidfile
            sys.stderr.write(msg)
            if os.path.exists(self.pidfile):
                os.remove(self.pidfile)
            return
        #try to kill the daemon process
        try:
            i = 0
            while 1:
                os.kill(pid, signal.SIGTERM)
                time.sleep(0.1)
                i = i + 1
                if i % 10 == 0:
                    os.kill(pid, signal.SIGHUP)
        except OSError, err:
            err = str(err)
            if err.find('No such process') > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print str(err)
                sys.exit(1)
            if self.verbose >= 1:
                print 'Stopped!'
 
    def restart(self, *args, **kwargs):
        self.stop()
        self.start(*args, **kwargs)
 
    def is_running(self):
        pid = self.get_pid()
        #print(pid)
        return pid and os.path.exists('/proc/%d' % pid)
 
    def run(self, *args, **kwargs):
        'NOTE: override the method in subclass'
        print 'base class run()'
        
if __name__ == '__main__':
    cfg = "/etc/kubevmm/config"
    if not os.path.exists(cfg):
        cfg = "/home/kubevmm/bin/config"
    config_raw = parser()
    config_raw.read(cfg)
    TOKEN = config_raw.get('Kubernetes', 'token_file')
    config.load_kube_config(config_file=TOKEN)
    print(get_field_in_kubernetes_by_index('cloudinit', 'cloudplus.io', 'v1alpha3', 'virtualmachines', ['metadata', 'labels']))