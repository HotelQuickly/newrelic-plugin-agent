"""
Kubernetes

"""
import logging
import pykube
import os
import math

from newrelic_plugin_agent.plugins import base

LOGGER = logging.getLogger(__name__)


class Kubernetes(base.Plugin):
    GUID = 'com.hotelquickly.newrelic_kubernetes_agent'

    def __init__(self, config, poll_interval, last_interval_values=None):
        super(Kubernetes, self).__init__(config, poll_interval, last_interval_values)

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["google_application_credentials"]
        self._kube_config_file = config["kube_config"]
        self._api = pykube.HTTPClient(pykube.KubeConfig.from_file(self._kube_config_file))

    def poll(self):
        """Poll the Kubernetes server"""
        LOGGER.info('Polling Kubernetes')

        self.run()
        self.finish()

    def run(self):
        try:
            self._run_core_report()
        except Exception as e:
            LOGGER.exception(e)
            self.derive_values = dict()
            self.gauge_values = dict()
        try:
            self._run_app_report("inv-system-live-1")
            self._run_app_report("msi-affiliate-live")
            self._run_app_report("msi-hc-live")
            self._run_app_report("msi-trivago-live")
            self._run_app_report("msi-tripadvisor-live")
        except Exception as e:
            LOGGER.exception(e)

    def _run_core_report(self):
        total_available_cpu_available_rounded = 0
        nodes = self._get_nodes()
        for node in nodes:
            node_name = node["metadata"]["name"]
            pods = self._get_non_terminated_pods(node_name)

            cpu_req_sum = 0
            for pod in pods:
                container = pod["spec"]["containers"][0]
                if "resources" in container and "cpu" in container["resources"]:
                    cpu_req = container["resources"]["requests"]["cpu"]
                    cpu_req = self._convert_resources_value_to_float(cpu_req)
                    cpu_req_sum += cpu_req

            cpu_req_sum_rounded = int(math.floor(cpu_req_sum))

            allocatable_cpu = int(node["status"]["allocatable"]["cpu"])
            allocatable_cpu_rounded = allocatable_cpu - 1

            self.add_gauge_value("Resources/CPU/Requests/Usage/Raw/%s" % node_name, "Core",
                                 cpu_req_sum)
            self.add_gauge_value("Resources/CPU/Requests/Usage/Rounded/%s" % node_name, "Core",
                                 cpu_req_sum_rounded)

            self.add_gauge_value("Resources/CPU/Requests/Available/Raw/%s" % node_name, "Core",
                                 allocatable_cpu_rounded - cpu_req_sum)
            self.add_gauge_value("Resources/CPU/Requests/Available/Rounded/%s" % node_name, "Core",
                                 allocatable_cpu_rounded - cpu_req_sum_rounded)

            self.add_gauge_value("Resources/CPU/Capacity/Raw/%s" % node_name, "Core",
                                 allocatable_cpu)
            self.add_gauge_value("Resources/CPU/Capacity/Rounded/%s" % node_name, "Core",
                                 allocatable_cpu_rounded)

            total_available_cpu_available_rounded += allocatable_cpu_rounded - cpu_req_sum_rounded
        self.add_gauge_value("Summary/Resources/CPU/Requests/Available/Rounded", "Core",
                             total_available_cpu_available_rounded)

    def _run_app_report(self, app_name):
        hpa = self._get_hpa(app_name)
        status = hpa[0]["status"]

        self.add_gauge_value("App/%s/HPA/CPU/CurrentPercentage" % app_name,
                             "Percent",
                             status["currentCPUUtilizationPercentage"])

        self.add_gauge_value("App/%s/HPA/Replica/Current" % app_name,
                             "Pod",
                             status["currentReplicas"])

        self.add_gauge_value("App/%s/HPA/Replica/Desired" % app_name,
                             "Pod",
                             status["desiredReplicas"])

    def _get_nodes_default_pool(self):
        return self._get_nodes({"cloud.google.com/gke-nodepool": "default-pool"})

    def _get_nodes(self, selector=None):
        nodes = pykube.Node.objects(self._api).filter(selector=selector)
        return nodes.response["items"]

    def _get_hpa(self, namespace=None):
        hpa = pykube.HorizontalPodAutoscaler.objects(self._api).filter(namespace=namespace)
        return hpa.response["items"]

    def _get_non_terminated_pods(self, node_name):
        running_pods = pykube.Pod.objects(self._api).filter(
            namespace=pykube.all,
            field_selector={
                "spec.nodeName": node_name,
                "status.phase": "Running",
            })
        pending_pods = pykube.Pod.objects(self._api).filter(
            namespace=pykube.all,
            field_selector={
                "spec.nodeName": node_name,
                "status.phase": "Pending",
            })

        return [] + \
            (running_pods.response["items"] if running_pods.response["items"] else []) + \
            (pending_pods.response["items"] if pending_pods.response["items"] else [])

    @staticmethod
    def _convert_resources_value_to_float(value):
        if value.endswith("m", 0):
            value = float(value.rstrip("m")) / 1000
        else:
            value = float(value)
        return value
