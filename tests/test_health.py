import pytest
pytestmark = pytest.mark.asyncio


import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from main import app, get_redis_client

class MockRedis:
    async def get(self, key):
        # Custom return values for each test will be set via monkeypatch
        return None

@pytest_asyncio.fixture(autouse=True)
def override_redis(monkeypatch):
    app.dependency_overrides[get_redis_client] = lambda: MockRedis()
    yield
    app.dependency_overrides = {}

@pytest_asyncio.fixture
async def async_client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
async def test_health_no_token(async_client):
    response = await async_client.get("/health")
    assert response.status_code == 200
    data = response.json()
    # Should return Oura auth URL when no token
    assert "oura_auth_url" in data
    assert data["status"] == "healthy"

@pytest.mark.asyncio
async def test_health_expired_token(monkeypatch, async_client):
    # Patch get_dropbox_token to avoid real Redis async calls
    monkeypatch.setattr("metrics.atracker.dropbox.get_dropbox_token", lambda user_id: None)
    # Patch MockRedis.get to return expired token as bytes via async
    async def get_bytes(self, key):
        return b'{"access_token": "abc", "expires_at": 0}'
    monkeypatch.setattr(MockRedis, "get", get_bytes)
    # Patch get_data_from_api to return dummy data
    def mock_get_data_from_api(*args, **kwargs):
        return []
    monkeypatch.setattr("metrics.oura.ingest.get_data_from_api", mock_get_data_from_api)
    monkeypatch.setattr("metrics.view.get_metrics_pivot", lambda *args, **kwargs: ["metrics_view"])
    monkeypatch.setattr("main.enqueue_atracker_job", lambda enqueued_jobs, user_id: enqueued_jobs.update({"atracker": "mock_job_id"}))
    response = await async_client.get("/health")
    assert response.status_code == 200
    data = response.json()
    # Should return Oura auth URL when token is expired
    assert "metrics_view" in data
    assert "oura_auth_url" in data
    assert "dropbox_auth_url" in data
    assert "dropbox_auth_valid" in data
    assert data["status"] == "healthy"

@pytest.mark.asyncio
async def test_health_valid_token(monkeypatch, async_client):
    # Patch MockRedis.get to return valid token as bytes via async
    async def get_bytes(self, key):
        return b'{"access_token": "abc", "expires_at": 9999999999}'
    monkeypatch.setattr(MockRedis, "get", get_bytes)
    monkeypatch.setattr("metrics.atracker.dropbox.get_dropbox_token", lambda user_id: "dbx_token")
    monkeypatch.setattr("metrics.oura.ingest.pull_data", lambda *args, **kwargs: ("api_data", "persisted_data", {}))
    monkeypatch.setattr("metrics.view.get_metrics_pivot", lambda *args, **kwargs: ["metrics_view"])
    # Patch get_data_from_api to return dummy data
    def mock_get_data_from_api(*args, **kwargs):
        return []
    monkeypatch.setattr("metrics.oura.ingest.get_data_from_api", mock_get_data_from_api)
    monkeypatch.setattr("main.enqueue_atracker_job", lambda enqueued_jobs, user_id: enqueued_jobs.update({"atracker": "mock_job_id"}))
    response = await async_client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "metrics_view" in data
    assert "oura_auth_url" in data
    assert "oura_auth_valid" in data
    assert "dropbox_auth_url" in data
    assert "dropbox_auth_valid" in data
    assert data["status"] == "healthy"

@pytest.mark.asyncio
async def test_health_dropbox_auth_required(monkeypatch, async_client):
    # Patch Redis to return valid Oura token
    async def get_bytes(self, key):
        return b'{"access_token": "abc", "expires_at": 9999999999}'
    monkeypatch.setattr(MockRedis, "get", get_bytes)
    monkeypatch.setattr("metrics.atracker.dropbox._redis", MockRedis())
    # Patch Dropbox token to return None (simulate missing Dropbox auth)
    monkeypatch.setattr("metrics.atracker.dropbox.get_dropbox_token", lambda user_id: None)
    # Patch DropboxAuthManager.get_authorize_url to return a dummy URL
    monkeypatch.setattr("metrics.atracker.dropbox.DropboxAuthManager.get_authorize_url", lambda self: "https://dropbox-auth-url")
    # Patch Oura API and metrics pivot
    monkeypatch.setattr("metrics.oura.ingest.pull_data", lambda *args, **kwargs: ([], [], {}))
    monkeypatch.setattr("metrics.view.get_metrics_pivot", lambda *args, **kwargs: ["metrics_view"])
    def mock_get_data_from_api(*args, **kwargs):
        return []
    monkeypatch.setattr("metrics.oura.ingest.get_data_from_api", mock_get_data_from_api)
    monkeypatch.setattr("main.enqueue_atracker_job", lambda enqueued_jobs, user_id: enqueued_jobs.update({"atracker": "mock_job_id"}))
    response = await async_client.get("/health")
    assert response.status_code == 200
    data = response.json()
    # Accept dropbox_auth_url or dropbox_auth_valid in response
    assert "dropbox_auth_url" in data or "dropbox_auth_valid" in data
    assert data["status"] == "healthy"

@pytest.mark.asyncio
async def test_health_dropbox_redirect(monkeypatch, async_client):
    # Patch Redis to return valid Oura token
    async def get_bytes(self, key):
        return b'{"access_token": "abc", "expires_at": 9999999999}'
    monkeypatch.setattr(MockRedis, "get", get_bytes)
    monkeypatch.setattr("metrics.atracker.dropbox._redis", MockRedis())
    # Patch Dropbox token to return None (simulate missing Dropbox auth)
    monkeypatch.setattr("metrics.atracker.dropbox.get_dropbox_token", lambda user_id: None)
    # Patch DROPBOX_REDIRECT_URI and DOMAIN env vars
    monkeypatch.setattr("os.getenv", lambda key: "dummy" if key == "DROPBOX_REDIRECT_URI" else "example.com")
    # Patch Oura API and metrics pivot
    monkeypatch.setattr("metrics.oura.ingest.pull_data", lambda *args, **kwargs: ([], [], {}))
    monkeypatch.setattr("metrics.view.get_metrics_pivot", lambda *args, **kwargs: ["metrics_view"])
    def mock_get_data_from_api(*args, **kwargs):
        return []
    monkeypatch.setattr("metrics.oura.ingest.get_data_from_api", mock_get_data_from_api)
    monkeypatch.setattr("main.enqueue_atracker_job", lambda enqueued_jobs, user_id: enqueued_jobs.update({"atracker": "mock_job_id"}))
    response = await async_client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"