from charms.reactive import when, when_not, set_state, is_state
from charms.reactive.helpers import data_changed
from charms.layer.hadoop_base import get_hadoop_base
from jujubigdata.handlers import HDFS
from jujubigdata import utils


@when('namenode.ready')
@when_not('datanode.started')
def start_datanode(namenode):
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)
    update_config(namenode)  # force config update
    hdfs.start_datanode()
    hdfs.start_journalnode()
    hadoop.open_ports('datanode')
    set_state('datanode.started')


@when('namenode.ready')
@when('datanode.started')
def send_jn_port(namenode):
    hadoop = get_hadoop_base()
    namenode.send_jn_port(hadoop.dist_config.port('journalnode'))


@when('namenode.ready')
@when('datanode.started')
def update_config(namenode):
    hadoop = get_hadoop_base()
    hdfs = HDFS(hadoop)

    utils.update_kv_hosts(namenode.hosts_map())
    utils.manage_etc_hosts()

    namenode_data = (
        namenode.clustername(), namenode.namenodes(),
        namenode.port(), namenode.webhdfs_port(),
    )
    if data_changed('datanode.namenode-data', namenode_data):
        hdfs.configure_datanode(*namenode_data)
        if is_state('datanode.started'):  # re-check because for manual call
            hdfs.restart_datanode()
            hdfs.restart_journalnode()

    if data_changed('datanode.namenode-ssh-key', namenode.ssh_key()):
        utils.install_ssh_key('hdfs', namenode.ssh_key())
