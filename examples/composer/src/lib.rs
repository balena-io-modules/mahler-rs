use anyhow::Context;
use bollard::container::CreateContainerOptions;
use bollard::secret::{ContainerInspectResponse, ContainerState, ContainerStateStatusEnum};
use futures_util::stream::StreamExt;
use gustav::worker::{Ready, Worker};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

use bollard::Docker;
use bollard::image::CreateImageOptions;

use gustav::extract::{Args, Pointer, Res, System, Target};
use gustav::task::prelude::*;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Default)]
pub enum ServiceStatus {
    #[default]
    Created,
    Running,
    Stopped,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TargetService {
    //// Image URL
    pub image: String,

    /// Service status
    #[serde(default)]
    pub status: ServiceStatus,

    /// Container command configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cmd: Option<Vec<String>>,
}

impl From<TargetService> for bollard::container::Config<String> {
    fn from(tgt: TargetService) -> bollard::container::Config<String> {
        bollard::container::Config {
            image: Some(tgt.image),
            cmd: tgt.cmd,
            ..Default::default()
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Service {
    /// Container id for the service
    pub id: Option<String>,

    //// Image URL
    pub image: Option<String>,

    /// Service status
    pub status: Option<ServiceStatus>,

    /// Container creation date, not used as part
    /// of the target state
    pub created_at: Option<String>,

    /// Container last start date, not used as part
    pub started_at: Option<String>,

    /// Container last stop date, not used as part
    /// of the target state
    pub finished_at: Option<String>,

    /// Container command configuration
    pub cmd: Option<Vec<String>>,
}

impl From<ContainerInspectResponse> for Service {
    fn from(info: ContainerInspectResponse) -> Self {
        let ContainerInspectResponse {
            id,
            image,
            created: created_at,
            state,
            config,
            ..
        } = info;

        // Get status info
        let (status, started_at, finished_at) = if let Some(ContainerState {
            status,
            started_at,
            finished_at,
            ..
        }) = state
        {
            let status = match status {
                Some(ContainerStateStatusEnum::RUNNING) => Some(ServiceStatus::Running),
                Some(ContainerStateStatusEnum::EXITED) => Some(ServiceStatus::Stopped),
                Some(ContainerStateStatusEnum::CREATED) => Some(ServiceStatus::Created),
                Some(_) => None, // ignore other statuses for now
                None => None,
            };

            (status, started_at, finished_at)
        } else {
            (None, None, None)
        };

        let cmd = config.and_then(|c| c.cmd);

        Self {
            id,
            image,
            status,
            created_at,
            started_at,
            finished_at,
            cmd,
        }
    }
}

impl From<TargetService> for Service {
    fn from(tgt: TargetService) -> Self {
        let TargetService {
            image,
            status,
            cmd: command,
        } = tgt;
        Self {
            id: None,
            image: Some(image),
            status: Some(status),
            created_at: None,
            started_at: None,
            finished_at: None,
            cmd: command,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Image {
    /// Image Id on the engine
    pub id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct TargetImage {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Project {
    /// Project name
    pub name: String,

    /// List of project services
    pub services: HashMap<String, Service>,

    /// List of managed images
    pub images: HashMap<String, Image>,
}

impl Project {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            services: HashMap::new(),
            images: HashMap::new(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TargetProject {
    /// Project name
    pub name: String,

    /// List of project services
    pub services: HashMap<String, TargetService>,
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct FetchImageError(#[from] anyhow::Error);

/// Pull an image from the registry, this task is applicable to
/// the creation of a new image
///
/// Condition: the image is not already present in the device
/// Effect: add the image to the list of images
/// Action: pull the image from the registry and add it to the images local registry
fn fetch_image(
    mut image: Pointer<Image>,
    Args(image_name): Args<String>,
    docker: Res<Docker>,
) -> New<Image, FetchImageError> {
    // Initialize the image if it doesn't exist
    if image.is_none() {
        image.zero();
    }

    with_io(image, |mut image| async move {
        // Check if the image exists first, we do this because
        // we don't know if the initial state is correct
        match docker.inspect_image(image_name.as_str()).await {
            Ok(img_info) => {
                if let Some(id) = img_info.id {
                    // If the image exists and has an id, skip
                    // download
                    image.assign(Image { id: Some(id) });
                    return Ok(image);
                }
            }
            Err(e) => {
                if let bollard::errors::Error::DockerResponseServerError { status_code, .. } = e {
                    if status_code != 404 {
                        return Err(e).with_context(|| {
                            format!("failed to read information for image {image_name}")
                        })?;
                    }
                } else {
                    return Err(e).with_context(|| {
                        format!("failed to read information for image {image_name}")
                    })?;
                }
            }
        }

        // Otherwise try to download the image
        let options = Some(CreateImageOptions {
            from_image: image_name.clone(),
            ..Default::default()
        });

        // Try to create the image
        let mut stream = docker.create_image(options, None, None);
        while let Some(progress) = stream.next().await {
            let _ = progress.with_context(|| format!("failed to download image {image_name}"))?;
        }

        // Check that the image
        let img_info = docker
            .inspect_image(image_name.as_str())
            .await
            .with_context(|| format!("failed to read information for image {image_name}"))?;

        image.assign(Image { id: img_info.id });

        Ok(image)
    })
}

/// Pull an image from the registry, this task is applicable to
/// the creation of a service, as pulling an image is only needed
/// in that case.
fn fetch_service_image(Target(tgt): Target<TargetService>) -> Task {
    fetch_image.with_arg("image_name", tgt.image)
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct InstallServiceError(#[from] anyhow::Error);

/// Create a new service container from service data
///
/// Condition: the service is not already present in the `services` object and the service image has already been downloaded
/// Effect: add the service to the `services` object, with a `status` of `created`
/// Action: create a new container using the docker API and set the `containerId` property of the service in the `services` object
fn install_service(
    mut svc: Pointer<Service>,
    Args(service_name): Args<String>,
    System(project): System<Project>,
    Target(tgt): Target<TargetService>,
    docker: Res<Docker>,
) -> New<Service, InstallServiceError> {
    let tgt_img = tgt.image.clone();
    let local_img = project.images.get(tgt_img.as_str());

    if local_img.is_some() {
        svc.assign(Service {
            // The status should be 'Created' after install
            status: Some(ServiceStatus::Created),
            // The rest of the fields should be the same
            ..tgt.clone().into()
        });
    }

    // Clone the local image to move it into the async block
    let local_img = local_img.cloned().and_then(|i| i.id);

    with_io(svc, |mut svc| async move {
        let container_name = format!("{}_{}", project.name, service_name);
        match docker
            .inspect_container(container_name.as_str(), None)
            .await
        {
            Ok(svc_info) => {
                let mut existing: Service = svc_info.into();

                // If the existing service has the same image id as the
                // locally stored image, then we replace it with the target img
                // name
                if existing.image.is_some() && existing.image == local_img {
                    existing.image = Some(tgt_img);
                }

                svc.assign(existing);

                return Ok(svc);
            }
            Err(e) => {
                if let bollard::errors::Error::DockerResponseServerError { status_code, .. } = e {
                    if status_code != 404 {
                        return Err(e).with_context(|| {
                            format!(
                                "failed to read container information for service {service_name}"
                            )
                        })?;
                    }
                } else {
                    return Err(e).with_context(|| {
                        format!("failed to read container information for service {service_name}")
                    })?;
                }
            }
        };

        let options = Some(CreateContainerOptions {
            name: container_name.clone(),
            platform: None,
        });

        docker
            .create_container(options, tgt.into())
            .await
            .with_context(|| format!("failed to create container for service {service_name}"))?;

        // Look for the new service
        let mut new_svc: Service = docker
            .inspect_container(container_name.as_str(), None)
            .await
            .with_context(|| {
                format!("failed to read container information for service {service_name}")
            })?
            .into();

        // If the existing service has the same image id as the
        // locally stored image, then we replace it with the target img
        // name
        if new_svc.image.is_some() && new_svc.image == local_img {
            new_svc.image = Some(tgt_img);
        }

        // Assign the pointer to the new service info
        svc.assign(new_svc);

        Ok(svc)
    })
}

pub fn create_worker(project: Project) -> Worker<Project, Ready, TargetProject> {
    // Initialize the connection
    let docker = Docker::connect_with_local_defaults().unwrap();

    Worker::new()
        .job(
            "/images/{image_name}",
            create(fetch_image).with_description(|Args(image_name): Args<String>| {
                format!("pull image '{image_name}'")
            }),
        )
        .jobs(
            "/services/{service_name}",
            [
                create(fetch_service_image),
                create(install_service).with_description(|Args(service_name): Args<String>| {
                    format!("install container for service '{service_name}'")
                }),
            ],
        )
        .resource::<Docker>(docker)
        .initial_state(project)
        .unwrap()
}

#[cfg(test)]
mod tests {
    use bollard::container::ListContainersOptions;
    use gustav::worker::SeekStatus;
    use serde_json::json;
    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::{EnvFilter, prelude::*};

    use super::*;

    fn before() {
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .pretty()
                    .with_target(false)
                    .with_thread_names(true)
                    .with_thread_ids(true)
                    .with_line_number(true)
                    .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE),
            )
            .with(EnvFilter::from_default_env())
            .try_init()
            .unwrap_or(());
    }

    async fn after() {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let containers = docker
            .list_containers(Some(ListContainersOptions::<String> {
                all: true,
                ..Default::default()
            }))
            .await
            .map(|containers| {
                containers
                    .into_iter()
                    .filter(|c| {
                        c.names
                            .iter()
                            .any(|names| names.iter().any(|name| name.starts_with("/my-project_")))
                    })
                    .collect()
            })
            .unwrap_or(vec![]);

        // Delete containers
        for c in containers {
            if let Some(id) = c.id {
                docker.delete_service(id.as_str()).await.unwrap_or(());
            }
        }

        // Delete unused images
        let mut filters = HashMap::new();
        filters.insert("dangling", vec!["false"]);
        let _ = docker
            .prune_images(Some(bollard::image::PruneImagesOptions { filters }))
            .await;
    }

    #[tokio::test]
    async fn test_fetch_image() {
        before();

        let worker = create_worker(Project::new("my_project"));
        let state = worker
            .run_task(fetch_image.with_arg("image_name", "alpine:3.18"))
            .await
            .unwrap();

        // The alpine image must exist now
        let docker = Docker::connect_with_local_defaults().unwrap();
        let img = docker.inspect_image("alpine:3.18").await.unwrap();
        assert_eq!(state.images.get("alpine:3.18").unwrap().id, img.id);

        // cleanup
        after().await
    }

    #[tokio::test]
    async fn test_create_container() {
        before();

        let worker = create_worker(Project::new("my_project"));
        let target = serde_json::from_value::<TargetProject>(json!({
            "name": "my_project",
            "services": {
                "my-service": {
                    "image": "alpine:3.18",
                    "cmd": ["sleep", "infinity"]
                }
            }
        }))
        .unwrap();
        println!("{:?}", target);

        // Seeking the target must succeed
        let worker = worker.seek_target(target).await.unwrap();
        assert_eq!(worker.status(), &SeekStatus::TargetStateReached);

        // The alpine image must exist now
        let docker = Docker::connect_with_local_defaults().unwrap();
        let img = docker.inspect_image("alpine:3.18").await.unwrap();

        // The image ids should match
        let state = worker.state().await.unwrap();
        assert_eq!(state.images.get("alpine:3.18").unwrap().id, img.id);

        let container = docker
            .inspect_container("my_project_my-service", None)
            .await
            .unwrap();
        assert_eq!(container.id, state.services.get("my-service").unwrap().id);
        assert_eq!(
            container.state.unwrap().status,
            Some(ContainerStateStatusEnum::CREATED)
        );

        // cleanup
        after().await
    }
}
