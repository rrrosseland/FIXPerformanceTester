#!/usr/bin/env bash
echo "== DOCKER INFO =="; docker info --format '{{json .}}' | jq -r '.ServerVersion as $v | "Version: \($v)"' 2>/dev/null || docker info
echo -e "\n== RUNNING CONTAINERS =="; docker ps --size --format 'table {{.Names}}\t{{.Image}}\t{{.Ports}}\t{{.Size}}'
echo -e "\n== ALL CONTAINERS =="; docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}\t{{.CreatedAt}}'
echo -e "\n== TOP IMAGES BY SIZE =="; docker images --format '{{.Repository}}:{{.Tag}}\t{{.Size}}' | sort -h | tail -n 20
echo -e "\n== STORAGE =="; docker system df -v
echo -e "\nTip: Inspect labels for origin (Compose/Swarm/etc.): docker inspect <container> | jq .[0].Config.Labels"

