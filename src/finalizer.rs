use crate::crd::Volume;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client, Error};
use serde_json::{json, Value};

/// Adds a finalizer record into an `Volume` kind of resource. If the finalizer already exists,
/// this action has no effect.
///
/// # Arguments:
/// - `client` - Kubernetes client to modify the `Volume` resource with.
/// - `name` - Name of the `Volume` resource to modify. Existence is not verified
/// - `namespace` - Namespace where the `Volume` resource with given `name` resides.
///
/// Note: Does not check for resource's existence for simplicity.
pub async fn add(client: Client, name: &str, namespace: &str) -> Result<Volume, Error> {
    let api: Api<Volume> = Api::namespaced(client, namespace);
    let finalizer: Value = json!({
        "metadata": {
            "finalizers": ["volumes.example.com/finalizer"]
        }
    });

    let patch: Patch<&Value> = Patch::Merge(&finalizer);
    Ok(api.patch(name, &PatchParams::default(), &patch).await?)
}

/// Removes all finalizers from an `Volume` resource. If there are no finalizers already, this
/// action has no effect.
///
/// # Arguments:
/// - `client` - Kubernetes client to modify the `Volume` resource with.
/// - `name` - Name of the `Volume` resource to modify. Existence is not verified
/// - `namespace` - Namespace where the `Volume` resource with given `name` resides.
///
/// Note: Does not check for resource's existence for simplicity.
pub async fn delete(client: Client, name: &str, namespace: &str) -> Result<Volume, Error> {
    let api: Api<Volume> = Api::namespaced(client, namespace);
    let finalizer: Value = json!({
        "metadata": {
            "finalizers": null
        }
    });

    let patch: Patch<&Value> = Patch::Merge(&finalizer);
    Ok(api.patch(name, &PatchParams::default(), &patch).await?)
}
