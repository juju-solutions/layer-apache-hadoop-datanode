from charms.reactive import when, when_not, set_state, is_state, remove_state
from charms.hadoop import get_hadoop_base
from jujubigdata.handlers import HDFS
from jujubigdata import utils
from charmhelpers.core import hookenv


@when('hadoop.installed')
@when_not('spec.mismatch', 'namenode.registered', 'datanode.started')
def mark_blocked_waiting():
    from charms.reactive.bus import get_states
    hookenv.log('States: {}'.format(get_states().keys()))
    if not is_state('namenode.related'):
        hookenv.status_set('blocked', 'Waiting for relation to NameNode')
    else:
        hookenv.status_set('waiting', 'Waiting for NameNode')


@when_not('namenode.available')
def no_spec():
    # unavailable spec can neither be confirmed nor denied
    remove_state('spec.verified')
    remove_state('spec.mismatch')


@when('namenode.available')
def verify_spec(namenode):
    hadoop = get_hadoop_base()
    if utils.spec_matches(hadoop.spec(), namenode.spec()):
        set_state('spec.verified')
        remove_state('spec.mismatch')
    else:
        hookenv.log('Spec mismatch: {} != {}'.format(hadoop.spec(), namenode.spec()))
        hookenv.status_set('blocked',
                           'Spec mismatch with NameNode: {} != {}'.format(
                               hadoop.spec(), namenode.spec()))
        remove_state('spec.verified')
        set_state('spec.mismatch')


@when('spec.verified', 'namenode.available')
def update_etc_hosts(namenode):
    utils.update_kv_hosts(namenode.hosts_map())
    utils.manage_etc_hosts()


@when('spec.verified', 'namenode.available')
@when_not('namenode.registered')
def register_datanode(namenode):
    namenode.register_datanode()


@when('spec.verified', 'namenode.registered')
@when_not('datanode.started')
def start_datanode(namenode):
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    hdfs.configure_datanode(namenode.host(), namenode.port())
    hdfs.start_datanode()
    hadoop.open_ports('datanode')
    set_state('datanode.started')


@when_not('spec.verified', 'namenode.available')
def stop_datanode():
    if is_state('datanode.started'):
        hadoop = get_hadoop_base()
        hdfs = HDFS(hadoop)
        hdfs.stop_datanode()
        hadoop.close_ports('datanode')
        remove_state('datanode.started')


@when('datanode.started')
def mark_active():
    hookenv.status_set('active', 'Ready')
