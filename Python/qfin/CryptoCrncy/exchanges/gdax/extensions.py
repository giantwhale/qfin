from gdax.authenticated_client import AuthenticatedClient
from gdax.authenticated_client import requests
from gdax.authenticated_client import json

class AuthenticatedClientEnriched(AuthenticatedClient):

    def get_orders(self, status=None):
        result = []
        r = requests.get(self.url + '/orders/', auth=self.auth)
        if status is not None:
            assert isinstance(status, (str, list))
            if not isinstance(status, list):
                status = [status]
            query_args = '/orders?' + '&'.join(['status=' + s for s in status])
            r = requests.get(self.url + query_args, auth=self.auth)
        # r.raise_for_status()
        result.append(r.json())
        if 'cb-after' in r.headers:
            self.paginate_orders(result, r.headers['cb-after'])
        return result

    def cancel_all(self, data=None, product=''):
        if type(data) is dict:
            if "product" in data:
                product = data["product"]
        r = requests.delete(self.url + '/orders/',
                            # @Old: 
                            # data=json.dumps({'product_id': product or self.product_id}), auth=self.auth)
                            # @New:
                            data=json.dumps({'product_id': ''}), auth=self.auth)
        # r.raise_for_status()
        return r.json()
