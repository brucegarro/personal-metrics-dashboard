import webbrowser

from oura import OuraClient, OuraOAuth2Client

OURA_CLIENT_ID = os.environ["OURA_CLIENT_ID"]
OURA_CLIENT_SECRET = os.environ["OURA_CLIENT_SECRET"]
REDIRECT_URI="https://56c5e9241c3a.ngrok-free.app/oura_callback"

class OuraMetrics:
    def __init__(self):
        self.auth_client = OuraOAuth2Client(client_id=OURA_CLIENT_ID, client_secret=OURA_CLIENT_SECRET)

    def fetch_access_token(self):
        url, _ = self.auth_client.authorize_endpoint(scope="All scopes", redirect_uri=REDIRECT_URI)
        webbrowser.open(url)

    def handle_callback(self):
        token_dict = self.auth_client.fetch_access_token(code=code)
        access_token = token_dict.get("access_token")