"""Declarative collector registry."""

from dataclasses import dataclass
from typing import Type

from collectors.akash import AkashCollector
from collectors.aws import AWSCollector
from collectors.azure import AzureCollector
from collectors.base import BaseCollector
from collectors.browser_providers import (
    AethirBrowserCollector,
    CoreWeaveBrowserCollector,
    GMICloudBrowserCollector,
    GcoreBrowserCollector,
    HyperstackBrowserCollector,
    LightningAIBrowserCollector,
    QubridBrowserCollector,
    SaladBrowserCollector,
    TogetherBrowserCollector,
)
from collectors.cloreai import CloreAICollector
from collectors.crusoe import CrusoeCollector
from collectors.cudo import CudoCollector
from collectors.datacrunch import DataCrunchCollector
from collectors.deepinfra import DeepInfraCollector
from collectors.denvr import DenvrCollector
from collectors.e2e import E2ECollector
from collectors.gcp import GCPCollector
from collectors.getdeploying import GetDeployingCollector
from collectors.jarvislabs import JarvisLabsCollector
from collectors.lambda_cloud import LambdaCollector
from collectors.latitude import LatitudeCollector
from collectors.linode import LinodeCollector
from collectors.massedcompute import MassedComputeCollector
from collectors.novita import NovitaCollector
from collectors.openrouter import OpenRouterCollector
from collectors.oracle import OracleCollector
from collectors.paperspace import PaperspaceCollector
from collectors.primeintellect import PrimeIntellectCollector
from collectors.runpod import RunPodCollector
from collectors.shadeform import ShadeformCollector
from collectors.skypilot import SkyPilotCollector
from collectors.tensordock import TensorDockCollector
from collectors.thundercompute import ThunderComputeCollector
from collectors.vastai import VastAICollector
from collectors.voltagepark import VoltageParkCollector
from collectors.vultr import VultrCollector


@dataclass(frozen=True)
class CollectorSpec:
    name: str
    collector_cls: Type[BaseCollector]
    kind: str
    active: bool = True


COLLECTOR_SPECS = [
    # No-auth APIs
    CollectorSpec("aws", AWSCollector, "api"),
    CollectorSpec("azure", AzureCollector, "api"),
    CollectorSpec("oracle", OracleCollector, "api"),
    CollectorSpec("tensordock", TensorDockCollector, "api"),
    CollectorSpec("skypilot", SkyPilotCollector, "api"),
    # No-auth static scrapers
    CollectorSpec("getdeploying", GetDeployingCollector, "scraper"),
    CollectorSpec("jarvislabs", JarvisLabsCollector, "scraper"),
    CollectorSpec("thundercompute", ThunderComputeCollector, "scraper"),
    CollectorSpec("crusoe", CrusoeCollector, "scraper"),
    CollectorSpec("novita", NovitaCollector, "scraper"),
    CollectorSpec("akash", AkashCollector, "scraper"),
    CollectorSpec("cudo", CudoCollector, "scraper"),
    CollectorSpec("vultr", VultrCollector, "scraper"),
    CollectorSpec("paperspace", PaperspaceCollector, "scraper"),
    CollectorSpec("deepinfra", DeepInfraCollector, "scraper"),
    CollectorSpec("linode", LinodeCollector, "scraper"),
    CollectorSpec("latitude", LatitudeCollector, "scraper"),
    CollectorSpec("massedcompute", MassedComputeCollector, "scraper"),
    CollectorSpec("e2e", E2ECollector, "scraper"),
    CollectorSpec("voltagepark", VoltageParkCollector, "scraper"),
    CollectorSpec("denvr", DenvrCollector, "scraper"),
    CollectorSpec("cloreai", CloreAICollector, "scraper"),
    # Browser scrapers
    CollectorSpec("coreweave", CoreWeaveBrowserCollector, "browser"),
    CollectorSpec("together", TogetherBrowserCollector, "browser"),
    CollectorSpec("hyperstack", HyperstackBrowserCollector, "browser"),
    CollectorSpec("gcore", GcoreBrowserCollector, "browser"),
    CollectorSpec("gmicloud", GMICloudBrowserCollector, "browser"),
    CollectorSpec("lightningai", LightningAIBrowserCollector, "browser"),
    CollectorSpec("salad", SaladBrowserCollector, "browser"),
    CollectorSpec("aethir", AethirBrowserCollector, "browser"),
    CollectorSpec("qubrid", QubridBrowserCollector, "browser"),
    # API-key collectors
    CollectorSpec("shadeform", ShadeformCollector, "api-key"),
    CollectorSpec("runpod", RunPodCollector, "api-key"),
    CollectorSpec("vastai", VastAICollector, "api-key"),
    CollectorSpec("lambda", LambdaCollector, "api-key"),
    CollectorSpec("gcp", GCPCollector, "api-key"),
    CollectorSpec("primeintellect", PrimeIntellectCollector, "api-key"),
    CollectorSpec("datacrunch", DataCrunchCollector, "api-key"),
    # Inactive collectors are explicit so source files cannot drift unnoticed.
    CollectorSpec("openrouter", OpenRouterCollector, "api", active=False),
]

ACTIVE_COLLECTOR_SPECS = [spec for spec in COLLECTOR_SPECS if spec.active]
COLLECTORS = {spec.name: spec.collector_cls for spec in ACTIVE_COLLECTOR_SPECS}
NO_AUTH_COLLECTORS = [
    spec.name for spec in ACTIVE_COLLECTOR_SPECS if spec.kind in {"api", "scraper"}
]
BROWSER_COLLECTORS = [spec.name for spec in ACTIVE_COLLECTOR_SPECS if spec.kind == "browser"]
API_KEY_COLLECTORS = [spec.name for spec in ACTIVE_COLLECTOR_SPECS if spec.kind == "api-key"]
COLLECTOR_TYPES = {spec.name: spec.kind for spec in COLLECTOR_SPECS}
