# Docker, git, curl
sudo dnf update -y
sudo dnf install -y docker git curl

# start docker and let ec2-user run it
sudo systemctl enable --now docker
sudo usermod -aG docker ec2-user
newgrp docker   # or log out/in

# Install Docker Compose v2 plugin (pick arch)
VER=v2.27.0
ARCH=$(uname -m); [ "$ARCH" = "x86_64" ] && BINARCH=linux-x86_64 || BINARCH=linux-aarch64
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL "https://github.com/docker/compose/releases/download/${VER}/docker-compose-${BINARCH}" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

# sanity
docker --version
docker compose version
git --version
curl --version