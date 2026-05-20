#!/usr/bin/env python3

import argparse
from pathlib import Path

def generate(path, host_ip, n_brokers = 3, container_id = "my_container_id"):
#     with Path(path, "kafka_pv.yaml").open("w", encoding="utf-8", newline="\n") as f:
#         f.write(
# """
# apiVersion: v1
# kind: PersistentVolume
# metadata:
#   name: kafka-pv-0
# spec:
#   capacity:
#     storage: 10Gi
#   volumeMode: Filesystem
#   storageClassName: local-path
#   accessModes:
#     - ReadWriteOnce
#   hostPath:
#     path: /home/ilia/kubernetes/data/kafka-0
#   persistentVolumeReclaimPolicy: Retain
# """
#         )
#  pvc will be used as a template
#    with Path(path, "kafka_pvc.yaml").open("w", encoding="utf-8", newline="\n") as f:
#        f.write(
#"""
#apiVersion: v1
#kind: PersistentVolumeClaim
#metadata:
#  name: kafka-pvc
#  namespace: kafka
#spec:
#  accessModes:
#    - ReadWriteOnce
#  volumeMode: Filesystem
#  storageClassName: local-path
#  resources:
#    requests:
#      storage: 10Gi
#"""
#        )

    ips = []
    origin_ip = host_ip.split(".")
    for i in range(n_brokers):
      parts = origin_ip
      parts[-1] = str(int(origin_ip[-1]) + i + 1)
      new_ip = ".".join(parts)
      ips.append(new_ip)

    with Path(path, "metal_ip_pool.yaml").open("w", encoding="utf-8", newline="\n") as f:
        f.write(
f"apiVersion: metallb.io/v1beta1\n\
kind: IPAddressPool\n\
metadata:\n\
  name: kafka-pool\n\
  namespace: metallb-system\n\
spec:\n\
  addresses:\n\
    - {host_ip}-{ips[-1]}"
        )
        
    with Path(path, "l2_advertisement.yaml").open("w", encoding="utf-8", newline="\n") as f:
        f.write(
"""
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: kafka-l2
  namespace: metallb-system
"""
        )

    with Path(path, "kafka_internal_svc.yaml").open("w", encoding="utf-8", newline="\n") as f:
        f.write(
"""
apiVersion: v1
kind: Service
metadata:
  name: kafka-internal
  namespace: kafka
spec:
  clusterIP: None
  selector:
    app: kafka
  ports:
    - name: broker
      port: 9092
    - name: controller
      port: 9093
"""
        )

    for i in range(n_brokers):
      ip = ips[i]
      with Path(path, f"kafka_external_svc_{i}.yaml").open("w", encoding="utf-8", newline="\n") as f:
          f.write(
f"apiVersion: v1\n\
kind: Service\n\
metadata:\n\
  name: kafka-{i}-external\n\
  namespace: kafka\n\
spec:\n\
  type: LoadBalancer\n\
  loadBalancerIP: {ip}\n\
  selector:\n\
    statefulset.kubernetes.io/pod-name: kafka-{i}\n\
  ports:\n\
    - port: 9094\n\
      targetPort: 9094"
          )

    with Path(path, "kafka_config_map.yaml").open("w", encoding="utf-8", newline="\n") as f:
        f.write(
f"apiVersion: v1\n\
kind: ConfigMap\n\
metadata:\n\
  name: kafka-config\n\
  namespace: kafka\n\
data:\n\
  server.properties.tpl: |\n\
    node.id={{{{BROKER_ID}}}}\n\
    process.roles=broker,controller\n\
    cluster.id={container_id}\n\
    controller.quorum.voters="
        )
        for i in range(n_brokers):
          if i == n_brokers - 1:
            f.write(f"{i}@kafka-{i}.kafka-internal.kafka.svc.cluster.local:9093\n")
          else:
            f.write(f"{i}@kafka-{i}.kafka-internal.kafka.svc.cluster.local:9093,")
        f.write(
f"    listeners=INTERNAL://:9092,EXTERNAL://:9094,CONTROLLER://:9093\n\
    advertised.listeners=INTERNAL://{{{{POD_NAME}}}}.kafka-internal.kafka.svc.cluster.local:9092,EXTERNAL://{{{{EXTERNAL_IP}}}}:9094\n\
    listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT\n\
    inter.broker.listener.name=INTERNAL\n\
    controller.listener.names=CONTROLLER\n\
    log.dirs=/tmp/kafka-logs"
        )

    with Path(path, "kafka_stateful_set.yaml").open("w", encoding="utf-8", newline="\n") as f:
        f.write(
f"apiVersion: apps/v1\n\
kind: StatefulSet\n\
metadata:\n\
  name: kafka\n\
  namespace: kafka\n\
spec:\n\
  serviceName: kafka-internal\n\
  replicas: {n_brokers}\n\
  selector:\n\
    matchLabels:\n\
      app: kafka\n\
  template:\n\
    metadata:\n\
      labels:\n\
        app: kafka\n\
    spec:\n\
      volumes:\n\
        - name: config-template\n\
          configMap:\n\
            name: kafka-config\n\
\n\
        - name: config-generated\n\
          emptyDir: {{}}\n\
      initContainers:\n\
        - name: kafka-init\n\
          image: busybox\n\
          command: [\"sh\", \"-c\"]\n\
          args:\n\
            - |\n\
\n\
              set -e\n\
\n\
              POD_NAME=$(hostname)\n\
              BROKER_ID=$(echo $POD_NAME | grep -o '[0-9]\+$')\n\n"
        )
        for i in range(n_brokers):
          ip = ips[i]
          f.write(
f"              if [ \"$BROKER_ID\" == \"{i}\" ]; then EXTERNAL_IP={ip}; fi\n"
          )

        f.write(
f"\n              echo \"POD_NAME=$POD_NAME\"\n\
              echo \"BROKER_ID=$BROKER_ID\"\n\
\n\
              sed -e \"s/{{{{POD_NAME}}}}/$POD_NAME/g\" -e \"s/{{{{BROKER_ID}}}}/$BROKER_ID/g\" -e \"s/{{{{EXTERNAL_IP}}}}/$EXTERNAL_IP/g\" /config/server.properties.tpl > /generated/server.properties\n\
\n\
              echo \"Generated config:\"\n\
              cat /generated/server.properties\n\
\n\
          volumeMounts: \n\
            - name: config-template \n\
              mountPath: /config \n\
            - name: config-generated \n\
              mountPath: /generated \n\
      containers:\n\
        - name: kafka\n\
          image: apache/kafka:3.7.0\n\
          ports:\n\
            - containerPort: 9092\n\
            - containerPort: 9093\n\
            - containerPort: 9094\n\
\n\
          volumeMounts:\n\
            - name: config-generated\n\
              mountPath: /generated\n\
            - name: data\n\
              mountPath: /tmp/kafka-logs\n\
          command: [\"sh\", \"-c\"]\n\
          args:\n\
            - |\n\
              set -eux\n\
              echo \"===== CONFIG FILE =====\"\n\
              ls -lah /generated\n\
              cat /generated/server.properties\n\
\n\
              echo \"===== FORMATTING STORAGE =====\"\n\
              /opt/kafka/bin/kafka-storage.sh format -t {container_id} -c /generated/server.properties --ignore-formatted\n\
\n\
              echo \"===== STARTING KAFKA =====\"\n\
\n\
              cat /generated/server.properties\n\
\n\
              exec /opt/kafka/bin/kafka-server-start.sh /generated/server.properties\n\
\n\
  volumeClaimTemplates:\n\
  - metadata:\n\
      name: data\n\
    spec:\n\
      accessModes: [\"ReadWriteOnce\"]\n\
      volumeMode: Filesystem\n\
      storageClassName: local-path\n\
      resources:\n\
        requests:\n\
          storage: 10Gi"
      )

def main():
    parser = argparse.ArgumentParser(description="Generate yaml files to setup kafta test pods")
    parser.add_argument("--path", help="Path where yamls will be generated", default="/home/ilia/kubernetes")
    parser.add_argument("--host-ip", help="Current host IP", default="localhost")
   
    args = parser.parse_args()

    generate(args.path, args.host_ip)

if __name__ == "__main__":
    main()


