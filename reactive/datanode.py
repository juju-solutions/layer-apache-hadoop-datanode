from charms.reactive import when, when_not, set_state, remove_state
from charms.hadoop import get_hadoop_base
from jujubigdata.handlers import HDFS
from jujubigdata import utils
from charmhelpers.core import hookenv


@when('hadoop.installed')
@when_not('datanode.related')
def blocked():
    hookenv.status_set('blocked', 'Waiting for relation to NameNode')


@when('hadoop.installed', 'datanode.related')
def set_spec(datanode):
    hadoop = get_hadoop_base()
    datanode.set_spec(hadoop.spec())


@when('datanode.spec.mismatch')
def spec_mismatch(namenode):
    hadoop = get_hadoop_base()
    hookenv.status_set('blocked',
                       'Spec mismatch with NameNode: {} != {}'.format(
                           hadoop.spec(), namenode.spec()))


@when('hadoop.installed', 'datanode.related')
@when_not('datanode.ready', 'datanode.spec.mismatch')
def waiting(datanode):
    datanode.register()
    hookenv.status_set('waiting', 'Waiting for NameNode')


@when('datanode.ready')
@when_not('datanode.started')
def start_datanode(namenode):
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    hdfs.configure_datanode(namenode.host(), namenode.port())
    utils.install_ssh_key('ubuntu', namenode.ssh_key())
    hdfs.start_datanode()
    hadoop.open_ports('datanode')
    set_state('datanode.started')
    hookenv.status_set('active', 'Ready')


@when('datanode.started')
@when_not('namenode.ready')
def stop_datanode():
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    hdfs.stop_datanode()
    hadoop.close_ports('datanode')
    remove_state('datanode.started')
