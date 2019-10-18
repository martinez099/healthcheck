from healthcheck.check_suites.base_suite import BaseCheckSuite


class BdbChecks(BaseCheckSuite):
    """Check database configurations. Parameter maps: sizing1."""

    def check_oss_api(self, *_args, **_kwargs):
        """check for OSS cluster API"""
        bdbs = self.api.get('bdbs')
        results = []
        kwargs = {}
        for bdb in bdbs:
            if bdb['oss_cluster']:
                result = bdb['shards_placement'] == 'sparse' and bdb['proxy_policy'] == 'all-master-shards'
                kwargs[bdb['name']] = result
                results.append(result)

        return all(results), kwargs

    def _check_bdb_alert_settings(self, *_args, **_kwargs):
        """get database alert settings"""
        alerts = self.api.get('bdbs/alerts')

        return None, {'alerts': alerts}

    def check_bdbs(self, *_args, **_kwargs):
        """check databases accoring to given paramter maps"""
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
        result, kwargs = {}, {'name': bdb['name'], 'failed': {}}
        for k, v in _values.items():
            result[k] = v == bdb[k]
            if not result[k]:
                kwargs['not satisfied'][k] = bdb[k]

        return all(result.values()), kwargs
