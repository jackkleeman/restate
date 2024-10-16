// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

use restate_schema_api::component::{BasicComponentMetadata, ComponentMetadataResolver};

impl ComponentMetadataResolver for Schemas {
    fn resolve_latest_component_handler(
        &self,
        component_name: impl AsRef<str>,
        handler_name: impl AsRef<str>,
    ) -> Option<BasicComponentMetadata> {
        self.use_component_schema(component_name.as_ref(), |component_schemas| {
            if component_schemas
                .handlers
                .contains_key(handler_name.as_ref())
            {
                Some(BasicComponentMetadata {
                    public: component_schemas.location.is_ingress_available(),
                    ty: component_schemas.ty,
                })
            } else {
                None
            }
        })
        .flatten()
    }

    fn resolve_latest_component_type(
        &self,
        component_name: impl AsRef<str>,
    ) -> Option<ComponentType> {
        self.use_component_schema(component_name.as_ref(), |component_schemas| {
            component_schemas.ty
        })
    }

    fn resolve_latest_component(
        &self,
        component_name: impl AsRef<str>,
    ) -> Option<ComponentMetadata> {
        let name = component_name.as_ref();
        self.use_component_schema(name, |component_schemas| {
            component_schemas.as_component_metadata(name.to_owned())
        })
        .flatten()
    }

    fn list_components(&self) -> Vec<ComponentMetadata> {
        let schemas = self.0.load();
        schemas
            .components
            .iter()
            .filter_map(|(component_name, component_schemas)| {
                component_schemas.as_component_metadata(component_name.clone())
            })
            .collect()
    }
}
