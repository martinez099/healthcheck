from healthcheck.check_suites.base_suite import BaseCheckSuite


class BdbChecks(BaseCheckSuite):
    """Check Databases via API"""

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
