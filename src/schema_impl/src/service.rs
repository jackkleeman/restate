// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::Schemas;

use crate::schemas_impl::{ServiceInstanceType, ServiceLocation, ServiceSchemas};
use restate_schema_api::service::{MethodMetadata, ServiceMetadata, ServiceMetadataResolver};

impl ServiceMetadataResolver for Schemas {
    fn resolve_latest_service_metadata(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<ServiceMetadata> {
        self.use_service_schema(service_name.as_ref(), |service_schemas| {
            map_to_service_metadata(service_name.as_ref(), service_schemas)
        })
        .flatten()
    }

    fn list_services(&self) -> Vec<ServiceMetadata> {
        let schemas = self.0.load();
        schemas
            .services
            .iter()
            .filter_map(|(service_name, service_schemas)| {
                map_to_service_metadata(service_name, service_schemas)
            })
            .collect()
    }

    fn is_service_public(&self, service_name: impl AsRef<str>) -> Option<bool> {
        self.use_service_schema(service_name.as_ref(), |service_schemas| {
            service_schemas.location.is_ingress_available()
        })
    }
}

fn map_to_service_metadata(
    service_name: &str,
    service_schemas: &ServiceSchemas,
) -> Option<ServiceMetadata> {
    match &service_schemas.location {
        ServiceLocation::BuiltIn { .. } => None, // We filter out from this interface ingress only services
        ServiceLocation::ServiceEndpoint {
            latest_endpoint,
            public,
        } => Some(ServiceMetadata {
            name: service_name.to_string(),
            methods: service_schemas
                .methods
                .values()
                .map(|method_desc| MethodMetadata {
                    name: method_desc.name().to_string(),
                    input_type: method_desc.input().full_name().to_string(),
                    output_type: method_desc.output().full_name().to_string(),
                    key_field_number: match &service_schemas.instance_type {
                        ServiceInstanceType::Keyed {
                            service_methods_key_field_root_number,
                            ..
                        } => Some(
                            *service_methods_key_field_root_number
                                .get(method_desc.name())
                                .expect("Method must exist in the parsed service methods"),
                        ),
                        _ => None,
                    },
                })
                .collect(),
            instance_type: (&service_schemas.instance_type)
                .try_into()
                .expect("Checked in the line above whether this is a built-in service or not"),
            endpoint_id: latest_endpoint.clone(),
            revision: service_schemas.revision,
            public: *public,
        }),
    }
}
