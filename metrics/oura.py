import os

from oura import OuraClient, OuraOAuth2Client


OURA_CLIENT_ID = os.environ["OURA_CLIENT_ID"]
OURA_CLIENT_SECRET = os.environ["OURA_CLIENT_SECRET"]
OURA_REDIRECT_URI=os.environ["OURA_REDIRECT_URI"]


class OuraMetrics:
    def __init__(self):
        self.auth_client = OuraOAuth2Client(client_id=OURA_CLIENT_ID, client_secret=OURA_CLIENT_SECRET)
        self.auth_client.session.scope = "All scopes"
        self.auth_client.session.redirect_uri = OURA_REDIRECT_URI

    def get_oura_auth_url(self):
        url, state = self.auth_client.authorize_endpoint()
        return url

    def handle_callback(self, code: str):
        print("code: %s", code)
        token_dict = self.auth_client.fetch_access_token(code=code)
        access_token = token_dict.get("access_token")
        return access_token