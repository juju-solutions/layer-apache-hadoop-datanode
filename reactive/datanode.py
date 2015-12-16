from charms.reactive import when, when_not, set_state, remove_state
from charms.hadoop import get_hadoop_base
from jujubigdata.handlers import HDFS
from jujubigdata import utils
from charmhelpers.core import hookenv


@when('hadoop.installed')
@when_not('namenode.related')
def blocked():
    hookenv.status_set('blocked', 'Waiting for relation to NameNode')


@when('hadoop.installed', 'namenode.related')
def set_spec(namenode):
    hadoop = get_hadoop_base()
    namenode.set_datanode_spec(hadoop.spec())


@when('namenode.spec.mismatch')
def spec_mismatch(namenode):
    hookenv.status_set('blocked',
                       'Spec mismatch with NameNode: {} != {}'.format(
                           namenode.datanode_spec(), namenode.namenode_spec()))


@when('hadoop.installed', 'namenode.related')
@when_not('namenode.spec.mismatch', 'namenode.ready', 'datanode.started')
def waiting(namenode):  # pylint: disable=unused-argument
    hookenv.status_set('waiting', 'Waiting for NameNode')


@when('namenode.ready')
@when_not('datanode.started')
def start_datanode(namenode):
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    hdfs.configure_datanode(namenode.host(), namenode.port())
    utils.install_ssh_key('ubuntu', namenode.ssh_key())
    hdfs.start_datanode()
    namenode.register()
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
