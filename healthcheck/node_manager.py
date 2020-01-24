from healthcheck.render_engine import print_success, print_msg, print_error


class NodeManager(object):
    """
    Node Manager class.
    """
    singleton = None

    def __init__(self, _api, _ssh, _skip_checks=False):
        """
       :param _api: An API fetcher instance.
       :param _ssh: A SSH commander instance.
       :param _skip_checks: Boolean whether to skip connection checks, defaults to False.
       """
        self.api = _api
        self.ssh = _ssh
        if not _skip_checks:
            self.check_api_connectivity()
            self.check_ssh_connectivity()

        self.internal_addrs = {future.hostname: future.result() for future in
                               self.ssh.exec_on_all_hosts('hostname -I')}

        self.uids = {node['addr']: node['uid'] for node in self.api.get('nodes')}

    @classmethod
    def instance(cls, _api, _ssh):
        if not cls.singleton:
            cls.singleton = NodeManager(_api, _ssh)
        return cls.singleton

    def get_internal_addr(self, _hostname):
        return self.internal_addrs[_hostname]

    def get_internal_addrs(self):
        return self.internal_addrs

    def get_uid(self, _internal_addr):
        return self.uids[_internal_addr]

    def get_uids(self):
        return self.uids

    def check_api_connectivity(self):
        try:
            print_msg('checking API connectivity')
            fqdn = self.api.get_value('cluster', 'name')
            print_success(f'successfully connected to {fqdn}')
        except Exception as e:
            print_error('could not connect to Redis Enterprise REST-API:', e)
            exit(2)
        print_msg('')

    def check_ssh_connectivity(self):
        print_msg('checking SSH connectivity')
        for hostname in self.ssh.hostnames:
            try:
                self.ssh.exec_on_host('sudo -v', hostname)
                print_success(f'successfully connected to {hostname}')
            except Exception as e:
                print_error(f'could not connect to host {hostname}:', e)
                exit(3)
        print_msg('')
