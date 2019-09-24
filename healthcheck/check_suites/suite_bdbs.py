from healthcheck.check_suites.base_suite import BaseCheckSuite


class BdbChecks(BaseCheckSuite):
    """Check databases [params]"""

    def check_oss_api(self, *_args, **_kwargs):
        """check for OSS API"""
        bdbs = self.api.get('bdbs')
        results = []
        kwargs = {}
        for bdb in bdbs:
            if bdb['oss_cluster']:
                result = bdb['shards_placement'] == 'sparse' and bdb['proxy_policy'] == 'all-master-shards'
                kwargs[bdb['name']] = result
                results.append(result)

        return all(results), kwargs

    def check_bdb_alert_settings(self, *_args, **_kwargs):
        """get database alert settings"""
        alerts = self.api.get('bdbs/alerts')

        return None, {'alerts': alerts}

    def check_bdbs(self, *_args, **_kwargs):
        """check bdbs"""
        bdbs = self.api.get('bdbs')
        results = []
        for bdb in bdbs:
            if bdb['name'] in _kwargs:
                result = self._check_bdb(bdb['uid'], _kwargs[bdb['name']])
                results.append(result)
        return results

    def _check_bdb(self, _uid, _dict):
        f"""check bdb:{_uid}"""
        bdb = self.api.get(f'bdbs/{_uid}')
        result, kwargs = {}, {'bdb_id': _uid}
        for k, v in _dict.items():
            result[k] = v == bdb[k]
            kwargs[k] = bdb[k]

        return all(result.values()), kwargs
