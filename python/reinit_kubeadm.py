#!/usr/bin/env python3

import os
import subprocess
import sys
import time
import argparse
from pathlib import Path
from kubernetes import client, config

from prepare_kafka import generate

POD_CIDR = "192.168.100.0/24"

def run(cmd, check=True):
    print(f"\n>>> {cmd}")
    result = subprocess.run(
        cmd,
        shell=True,
        text=True,
    )

    if check and result.returncode != 0:
        print(f"\n[ERROR] Command failed: {cmd}")
        sys.exit(result.returncode)


def wait_for_api(): 
    print("\nWaiting for Kubernetes API...")

    for _ in range(60):
        result = subprocess.run(
            "kubectl get nodes",
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        if result.returncode == 0:
            print("Kubernetes API is ready.")
            return

        time.sleep(5)

    print("Timeout waiting for Kubernetes API.")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Reset kubeadm if vm ip was changed")
    parser.add_argument("--host-ip", required=True, help="Host ip", default="192.168.0.0")
    parser.add_argument("--calico-version", help="Calico version", default="v3.28.0")
    parser.add_argument("--path", help="Path for storage and generator artefacts", default="/home/ilia/kubernetes")
        
    args = parser.parse_args()

    print("==== Kubernetes reset + reinit ====\n")

    # Reset kubeadm
    run("sudo kubeadm reset -f")

    # Cleanup old state
    cleanup_paths = [
        "/etc/cni/net.d",
        "/var/lib/cni",
        "/var/lib/calico",
        "/var/lib/kubelet",
        "/opt/local-path-provisioner"
    ]

    for path in cleanup_paths:
        run(f"sudo rm -rf {path}")

    # Remove old kubeconfig
    home = os.path.expanduser("~")
    kube_dir = os.path.join(home, ".kube")

    run(f"rm -rf {kube_dir}")

    # Restart services
    run("sudo systemctl restart containerd")
    run("sudo systemctl restart kubelet")

    # Initialize cluster
    init_cmd = (
        f"sudo kubeadm init "
        f"--apiserver-advertise-address={args.host_ip} "
        f"--pod-network-cidr={POD_CIDR}"
    )

    run(init_cmd)

    # Setup kubeconfig
    run("mkdir -p ~/.kube")
    run("sudo cp /etc/kubernetes/admin.conf ~/.kube/config")
    run("sudo chown $(id -u):$(id -g) ~/.kube/config")

    # Wait API
    wait_for_api()

    CALICO_URL = (
        f"https://raw.githubusercontent.com/projectcalico/calico/"
        f"{args.calico_version}/manifests/calico.yaml"
    )

    # Install Calico
    run(f"kubectl apply -f {CALICO_URL}")

    # Allow scheduling on control-plane
    run(
        "kubectl taint nodes --all "
        "node-role.kubernetes.io/control-plane-",
        check=False,
    )

    print("\nWaiting for pods to stabilize...\n")
    time.sleep(20)

    # Final status
    run("kubectl get nodes -o wide", check=False)
    run("kubectl get pods -A", check=False)

    print("\n==== Kubernetes cluster ready ====\n")

    tmp_path = Path(args.path)
    tmp_path.mkdir(parents=True, exist_ok=True)

    local_pv = Path(args.path, "data", "kafka-0")
    local_pv.mkdir(parents=True, exist_ok=True)

    print("\n==== Generate kafka yamls ====\n")

    run("kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/master/deploy/local-path-storage.yaml", check=False)

    generate(args.path, args.host_ip)

    print("\n==== Apply kafka yamls ====\n")
    
    run("kubectl create namespace kafka", check=False)
    run("kubectl apply -f https://raw.githubusercontent.com/metallb/metallb/v0.14.5/config/manifests/metallb-native.yaml", check=False)
    print("\nWaiting for metallb to stabilize...\n")
    time.sleep(60)

    #run("kubectl apply -f https://strimzi.io/install/latest?namespace=kafka -n kafka", check=False)

    run(f"kubectl apply -f {args.path}/metal_ip_pool.yaml", check=False)
    run(f"kubectl apply -f {args.path}/l2_advertisement.yaml", check=False)
    run(f"kubectl apply -f {args.path}/kafka_internal_svc.yaml", check=False)
    for i in range(3):
        run(f"kubectl apply -f {args.path}/kafka_external_svc_{i}.yaml", check=False)
    run(f"kubectl apply -f {args.path}/kafka_config_map.yaml", check=False)
    #run(f"kubectl apply -f {args.path}/kafka_pv.yaml", check=False)
    #run(f"kubectl apply -f {args.path}/kafka_pvc.yaml", check=False)
    run(f"kubectl apply -f {args.path}/kafka_stateful_set.yaml", check=False)
    
    print("\nWaiting for pods to stabilize...\n")
    time.sleep(20)

    run("kubectl get pods -n kafka", check=False)
    run("kubectl get svc -n kafka", check=False)
    run("kubectl get pv -n kafka", check=False)
    run("kubectl get pvc -n kafka", check=False)

    print("\n==== Kafka configured ====\n")

if __name__ == "__main__":
    main()

