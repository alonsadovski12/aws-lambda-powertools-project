"""
AWS Service Discovery configuration retrieval and caching utility
"""


import json
import os
from typing import Any, Dict, Optional, Union
from uuid import uuid4

import boto3
from botocore.config import Config

from ...shared import constants
from ...shared.functions import resolve_env_var_choice
from .base import DEFAULT_MAX_AGE_SECS, DEFAULT_PROVIDERS, BaseProvider
from .exceptions import GetParameterError

CLIENT_ID = str(uuid4())


class ServiceDiscoveryProvider(BaseProvider):
    """
    AWS Service Discovery Provider

    Parameters
    ----------

    config: botocore.config.Config, optional
        Botocore configuration to pass during client initialization
    boto3_session : boto3.session.Session, optional
            Boto3 session to use for AWS API communication

    Example
    -------
    **Retrieves a parameter value from AWS Service Discovery**

        >>> from aws_lambda_powertools.utilities.parameters import ServiceDiscoveryProvider
        >>> service_discovery_provider = ServiceDiscoveryProvider()
        >>>
        >>> value = service_discovery_provider.get("service_id", InstanceId='instance_id")
        >>>
        >>> print(value)
        '{"attribute1": "value1", "attribute2": "value2"}'
        >>> value = service_discovery_provider.get("service_id", InstanceId="instance_id", Attribute="attribute1")
        >>>
        >>> print(value)
        "value1"

    **Retrieves a parameter value from Systems Manager Parameter Store in another AWS region**

        >>> from botocore.config import Config
        >>> from aws_lambda_powertools.utilities.parameters import ServiceDiscoveryProvider
        >>>
        >>> config = Config(region_name="us-west-1")
        >>> service_discovery_provider = ServiceDiscoveryProvider(config=config)
        >>> value = service_discovery_provider.get("service_id", InstanceId='instance_id")
        >>>
        >>> print(value)
        '{"attribute1": "value1", "attribute2": "value2"}'
        >>> value = service_discovery_provider.get("service_id", InstanceId="instance_id", Attribute="attribute1")
        >>>
        >>> print(value)
        "value1"

    **Retrieves multiple instances from Service Discovery service using NameSpace and ServiceName**

        >>> from aws_lambda_powertools.utilities.parameters import ServiceDiscoveryProvider
        >>> service_discovery_provider = ServiceDiscoveryProvider()
        >>>
        >>> values = service_discovery_provider.get_multiple("namespace", ServiceName="service_name")
        >>>
        >>> print(values)
        [{
            'InstanceId': 'instance_id',
            'NamespaceName': 'namespace',
            'ServiceName': 'service_name',
            'HealthStatus': 'UNKNOWN',
            'Attributes': {
                'attribute1': 'first_instance_value1',
                'attribute2': 'first_instance_value2',
            }
        },  {
            'InstanceId': 'instance_id',
            'NamespaceName': 'namespace',
            'ServiceName': 'service_name',
            'HealthStatus': 'UNKNOWN',
            'Attributes': {
                'attribute1': 'second_instance_value1',
                'attribute2': 'second_instance_value2',
            }
        }]
    """

    client: Any = None

    def __init__(
        self,
        config: Optional[Config] = None,
        boto3_session: Optional[boto3.session.Session] = None,
    ):
        """
        Initialize the App Config client
        """

        config = config or Config()
        session = boto3_session or boto3.session.Session()
        self.client = session.client("servicediscovery", config=config)

        super().__init__()

    def _get(self, name: str, **sdk_options) -> str:
        """
        Retrieve a parameter value from AWS Service Discover

        Parameters
        ----------
        name: str
            The ID of the service that the instance is associated with.
        sdk_options: dict
             Dictionary of options that will be passed to the Service Discovery get_instance API call.
             Must contain:
                InstanceID: of the relevant instance
                Attribute: the attribute value to bring

        """

        # Explicit arguments will take precedence over keyword arguments

        sdk_options["ServiceId"] = name
        response = self.client.get_instance(ServiceId=sdk_options["ServiceId"], InstanceId=sdk_options["InstanceId"])[
            "Instance"
        ]["Attributes"]
        if sdk_options.get("Attribute"):
            return response.get(sdk_options.get("Attribute"))
        else:
            return json.dumps(response)

    def _get_multiple(self, path: str, **sdk_options) -> Dict[str, str]:
        """
        Retrieve a parameter value from AWS Systems Manager Parameter Store

        Parameters
        ----------
        path: str
            The HttpName name of the namespace.
        sdk_options: dict
             Dictionary of options that will be passed to the Service Discovery discover_instances API call.
             Must contain:
                ServiceName: The name of the service that you specified when you registered the instance.

        """
        sdk_options["NamespaceName"] = path
        return self.client.discover_instances(**sdk_options)["Instances"]


def get_service_attribute(
    name: str,
    transform: Optional[str] = None,
    force_fetch: bool = False,
    max_age: int = DEFAULT_MAX_AGE_SECS,
    **sdk_options
) -> Union[str, dict, bytes]:
    """
    Retrieve an instance value from AWS Service Discovery

    Parameters
    ----------
    name: str
        ID of the service to discover
    transform: str, optional
        Transforms the content from a JSON object ('json') or base64 binary string ('binary')
    force_fetch: bool, optional
        Force update even before a cached item has expired, defaults to False
    max_age: int
        Maximum age of the cached value
    sdk_options: dict
        Dictionary of options that will be passed to the Service Discovery get_instance API call.
        Must contain:
        InstanceID: of the relevant instance
        Attribute: the attribute value to bring

    Raises
    ------
    GetParameterError
        When the parameter provider fails to retrieve a parameter value for
        a given name.
    TransformParameterError
        When the parameter provider fails to transform a parameter value.

    Example
    -------
    **Retrieves an instance***

        >>> from aws_lambda_powertools.utilities.parameters import get_service_attribute
        >>>
        >>> get_service_attribute("service_id", InstanceId="instance_id")

    """

    # Only create the provider if this function is called at least once
    provider = "servicediscovery"
    if provider not in DEFAULT_PROVIDERS:
        DEFAULT_PROVIDERS[provider] = ServiceDiscoveryProvider()

    return DEFAULT_PROVIDERS[provider].get(
        name, max_age=max_age, transform=transform, force_fetch=force_fetch, **sdk_options
    )
