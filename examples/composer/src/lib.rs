use anyhow::{Context, anyhow};
use futures_util::stream::StreamExt;
use gustav::worker::{Ready, Worker};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

use bollard::Docker;
use bollard::image::CreateImageOptions;

use gustav::extract::{Args, Pointer, Res};
use gustav::task::prelude::*;

#[derive(Serialize, Deserialize, Debug)]
pub enum ServiceStatus {
    Created,
    Running,
    Stopped,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Service {
    /// Container id for the service
    #[serde(skip_serializing)]
    pub id: Option<String>,

    //// Image URL
    pub image: String,

    /// Service status
    pub status: Option<ServiceStatus>,

    /// Container creation date, not used as part
    /// of the target state
    pub created_at: Option<String>,

    /// Container last start date, not used as part
    /// of the target state
    pub started_at: Option<String>,

    /// Container last stop date, not used as part
    /// of the target state
    pub finished_at: Option<String>,

    /// Container command configuration
    pub command: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Image {
    /// Image Id on the engine
    pub id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
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

#[derive(Debug, Error)]
#[error(transparent)]
pub struct FetchImageError(#[from] anyhow::Error);

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
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code,
                message,
            }) => {
                if status_code != 404 {
                    return Err(FetchImageError(anyhow!(message)));
                }
            }
            Err(e) => return Err(e).with_context(|| "error inspecting image {image_name}")?,
        }

        // Otherwise try to download the image
        let options = Some(CreateImageOptions {
            from_image: image_name.clone(),
            ..Default::default()
        });

        let mut stream = docker.create_image(options, None, None);
        while let Some(progress) = stream.next().await {
            let _ = progress.with_context(|| "failed to download image {image_name}")?;
        }

        let img_info = docker
            .inspect_image(image_name.as_str())
            .await
            .with_context(|| "error inspecting image {image_name}")?;

        image.assign(Image { id: img_info.id });

        Ok(image)
    })
}

pub fn create_worker(project: Project) -> Worker<Project, Ready> {
    // Initialize the connection
    let docker = Docker::connect_with_local_defaults().unwrap();

    Worker::new()
        .job("/images/{image_name}", create(fetch_image))
        .resource::<Docker>(docker)
        .initial_state(project)
        .unwrap()
}

#[cfg(test)]
mod tests {
    use bollard::container::ListContainersOptions;

    use super::*;

    async fn cleanup() {
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
        let worker = create_worker(Project::new("my_project"));
        let state = worker
            .run_task(fetch_image.with_arg("image_name", "alpine:3.18"))
            .await
            .unwrap();

        let docker = Docker::connect_with_local_defaults().unwrap();
        let img = docker.inspect_image("alpine:3.18").await.unwrap();

        // The image must exist now
        assert_eq!(state.images.get("alpine:3.18").unwrap().id, img.id);

        // cleanup
        cleanup().await
    }
}
