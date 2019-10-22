from healthcheck.check_suites.base_suite import BaseCheckSuite, load_params


class BdbChecks(BaseCheckSuite):
    """Check database configurations"""

    def __init__(self, _config):
        super().__init__(_config)
        self.params = load_params('params_bdbs')

    def _connectivity_check(self):
        self._check_api_connectivity()

    def check_oss_api(self, *_args, **_kwargs):
        """check for OSS cluster API"""
        bdbs = self.api.get('bdbs')
        kwargs = {}
        for bdb in filter(lambda x: x['oss_cluster'], bdbs):
            kwargs[bdb['name']] = bdb['shards_placement'] == 'sparse' and bdb['proxy_policy'] == 'all-master-shards'

        return all(kwargs.values()), kwargs

    def _check_bdb_alert_settings(self, *_args, **_kwargs):
        """get database alert settings"""
        alerts = self.api.get('bdbs/alerts')

        return None, {'alerts': alerts}

    def check_bdbs(self, *_args, **_kwargs):
        """check databases according to given paramter map"""
        bdbs = self.api.get('bdbs')
        results = []
        for bdb in bdbs:
            values = _kwargs['__default__']
            if bdb['name'] in _kwargs:
                values.update(_kwargs[bdb['name']])
            result = self._check_bdb(bdb['uid'], values)
            results.append(result)

        return results

    def _check_bdb(self, _uid, _values):
        f"""check bdb:{_uid}"""
        bdb = self.api.get(f'bdbs/{_uid}')
        kwargs = {bdb['name']: {}}
        for k, v in _values.items():
            result = v == bdb[k]
            if not result:
                kwargs[bdb['name']][k] = bdb[k]

        return not any(kwargs[bdb['name']].values()), kwargs
