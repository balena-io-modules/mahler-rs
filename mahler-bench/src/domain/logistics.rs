//! Logistics: the classic HTN transport domain
//!
//! Packages have to be delivered between locations. Locations are grouped in cities,
//! each city has one truck and one of its locations is an airport, and airplanes move
//! between airports. This is the domain used in the AIPS-98 planning competition and
//! one of the standard examples for
//! [SHOP2](https://www.cs.umd.edu/~nau/papers/nau2003shop2.pdf).
//!
//! The `deliver` method plans one leg at a time — pick the package up, drive it to the
//! airport, fly it, deliver it locally — and the planner calls it again for whatever is
//! left, which is how a mahler domain expresses a recursive HTN method.
//!
//! Only the package locations are part of the target: trucks and airplanes are means,
//! not goals, so no diff is ever produced for them and their jobs can only be reached
//! through the method.

use mahler::extract::{Args, System, Target, View};
use mahler::job::{none, update};
use mahler::state::{List, Map, State};
use mahler::task::{Handler, Task};
use mahler::worker::{Ready, Worker};
use serde::{Deserialize, Serialize};

pub type Loc = String;

/// Where a package is: at a location, loaded in a truck or loaded in an airplane
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub enum PackageLoc {
    At(Loc),
    InTruck(String),
    InPlane(String),
}

impl State for PackageLoc {
    type Target = Self;
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct City {
    pub airport: Loc,
    pub locations: List<Loc>,
    pub truck: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct World {
    pub packages: Map<String, PackageLoc>,
    /// current location of each truck
    pub trucks: Map<String, Loc>,
    /// current airport of each airplane
    pub planes: Map<String, Loc>,
    /// static topology, not part of the target
    pub cities: Map<String, City>,
    /// index from location to the city it belongs to, not part of the target
    pub location_city: Map<Loc, String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorldTarget {
    pub packages: Map<String, PackageLoc>,
}

impl State for World {
    type Target = WorldTarget;
}

fn city_of<'s>(sys: &'s World, loc: &Loc) -> Option<&'s City> {
    let city = sys.location_city.get(loc)?;
    sys.cities.get(city)
}

/// The airplane to use to fly out of `airport`: one that is already there if possible
fn pick_plane(sys: &World, airport: &Loc) -> Option<String> {
    sys.planes
        .iter()
        .find(|(_, loc)| *loc == airport)
        .or_else(|| sys.planes.iter().next())
        .map(|(plane, _)| plane.clone())
}

/// Drive a truck to another location of its city
fn drive(mut loc: View<Loc>, Target(tgt): Target<Loc>) -> Option<View<Loc>> {
    if *loc == tgt {
        return None;
    }

    *loc = tgt;
    Some(loc)
}

/// Fly an airplane to another airport
fn fly(mut loc: View<Loc>, Target(tgt): Target<Loc>) -> Option<View<Loc>> {
    if *loc == tgt {
        return None;
    }

    *loc = tgt;
    Some(loc)
}

/// Load a package into the truck given as target, which must be at the package location
fn load_truck(
    mut pkg: View<PackageLoc>,
    Target(tgt): Target<PackageLoc>,
    System(sys): System<World>,
) -> Option<View<PackageLoc>> {
    let PackageLoc::InTruck(truck) = &tgt else {
        return None;
    };
    let PackageLoc::At(loc) = &*pkg else {
        return None;
    };

    if sys.trucks.get(truck) != Some(loc) {
        return None;
    }

    let tgt = tgt.clone();
    *pkg = tgt;
    Some(pkg)
}

/// Unload a package at the current location of the truck carrying it
fn unload_truck(mut pkg: View<PackageLoc>, System(sys): System<World>) -> Option<View<PackageLoc>> {
    let PackageLoc::InTruck(truck) = &*pkg else {
        return None;
    };

    let loc = sys.trucks.get(truck)?.clone();
    *pkg = PackageLoc::At(loc);
    Some(pkg)
}

/// Load a package into the airplane given as target, which must be at the package airport
fn load_plane(
    mut pkg: View<PackageLoc>,
    Target(tgt): Target<PackageLoc>,
    System(sys): System<World>,
) -> Option<View<PackageLoc>> {
    let PackageLoc::InPlane(plane) = &tgt else {
        return None;
    };
    let PackageLoc::At(loc) = &*pkg else {
        return None;
    };

    if sys.planes.get(plane) != Some(loc) {
        return None;
    }

    let tgt = tgt.clone();
    *pkg = tgt;
    Some(pkg)
}

/// Unload a package at the current airport of the airplane carrying it
fn unload_plane(mut pkg: View<PackageLoc>, System(sys): System<World>) -> Option<View<PackageLoc>> {
    let PackageLoc::InPlane(plane) = &*pkg else {
        return None;
    };

    let loc = sys.planes.get(plane)?.clone();
    *pkg = PackageLoc::At(loc);
    Some(pkg)
}

/// Drive `truck` to `to`, unless it is already there
fn drive_to(truck: &str, from: &Loc, to: &Loc) -> Option<Task> {
    (from != to).then(|| {
        drive
            .with_arg("truck", truck.to_string())
            .with_target(to.clone())
    })
}

/// Move a package between two locations of the same city with the city truck
fn truck_leg(sys: &World, city: &City, from: &Loc, to: &Loc) -> Vec<Task> {
    let truck_loc = match sys.trucks.get(&city.truck) {
        Some(loc) => loc,
        None => return Vec::new(),
    };

    let mut tasks = Vec::new();
    tasks.extend(drive_to(&city.truck, truck_loc, from));
    tasks.push(load_truck.with_target(PackageLoc::InTruck(city.truck.clone())));
    tasks.extend(drive_to(&city.truck, from, to));
    tasks.push(unload_truck.into_task());
    tasks
}

/// Plan the next leg of a delivery
fn deliver(
    pkg: View<PackageLoc>,
    Target(dest): Target<PackageLoc>,
    System(sys): System<World>,
) -> Vec<Task> {
    let PackageLoc::At(dest_loc) = &dest else {
        // only `at a location` is a valid goal for a package
        return Vec::new();
    };
    let Some(dest_city) = city_of(&sys, dest_loc) else {
        return Vec::new();
    };

    match &*pkg {
        // the package is on a truck: drop it at the destination if the truck can reach
        // it, otherwise at the airport of the city the truck is in
        PackageLoc::InTruck(truck) => {
            let Some(truck_loc) = sys.trucks.get(truck) else {
                return Vec::new();
            };
            let Some(truck_city) = city_of(&sys, truck_loc) else {
                return Vec::new();
            };

            let drop = if truck_city.airport == dest_city.airport {
                dest_loc
            } else {
                &truck_city.airport
            };

            let mut tasks = Vec::new();
            tasks.extend(drive_to(truck, truck_loc, drop));
            tasks.push(unload_truck.into_task());
            tasks
        }

        // the package is on an airplane: fly it to the destination airport
        PackageLoc::InPlane(plane) => {
            let Some(plane_loc) = sys.planes.get(plane) else {
                return Vec::new();
            };

            let mut tasks = Vec::new();
            if plane_loc != &dest_city.airport {
                tasks.push(
                    fly.with_arg("plane", plane.clone())
                        .with_target(dest_city.airport.clone()),
                );
            }
            tasks.push(unload_plane.into_task());
            tasks
        }

        PackageLoc::At(loc) => {
            let Some(city) = city_of(&sys, loc) else {
                return Vec::new();
            };

            // same city: one truck leg is the whole delivery
            if city.airport == dest_city.airport {
                if loc == dest_loc {
                    return Vec::new();
                }
                return truck_leg(&sys, city, loc, dest_loc);
            }

            // another city, and the package is not at an airport yet: bring it there
            if loc != &city.airport {
                return truck_leg(&sys, city, loc, &city.airport);
            }

            // at the airport of another city: fly it out
            let Some(plane) = pick_plane(&sys, loc) else {
                return Vec::new();
            };
            let Some(plane_loc) = sys.planes.get(&plane) else {
                return Vec::new();
            };

            let mut tasks = Vec::new();
            if plane_loc != loc {
                tasks.push(
                    fly.with_arg("plane", plane.clone())
                        .with_target(loc.clone()),
                );
            }
            tasks.push(load_plane.with_target(PackageLoc::InPlane(plane.clone())));
            tasks.push(
                fly.with_arg("plane", plane)
                    .with_target(dest_city.airport.clone()),
            );
            tasks.push(unload_plane.into_task());
            tasks
        }
    }
}

pub fn worker(initial: World) -> Worker<World, Ready> {
    Worker::new()
        .jobs(
            "/packages/{package}",
            [
                update(deliver),
                none(load_truck).with_description(|Args(package): Args<String>| {
                    format!("load {package} into truck")
                }),
                none(unload_truck).with_description(|Args(package): Args<String>| {
                    format!("unload {package} from truck")
                }),
                none(load_plane).with_description(|Args(package): Args<String>| {
                    format!("load {package} into plane")
                }),
                none(unload_plane).with_description(|Args(package): Args<String>| {
                    format!("unload {package} from plane")
                }),
            ],
        )
        .job(
            "/trucks/{truck}",
            none(drive).with_description(|Args(truck): Args<String>, Target(to): Target<Loc>| {
                format!("drive {truck} to {to}")
            }),
        )
        .job(
            "/planes/{plane}",
            none(fly).with_description(|Args(plane): Args<String>, Target(to): Target<Loc>| {
                format!("fly {plane} to {to}")
            }),
        )
        .initial_state(initial)
        .expect("logistics initial state should be valid")
}

fn location(city: usize, index: usize) -> Loc {
    format!("c{city}l{index}")
}

/// `packages` deliveries over `cities` cities of `locations` locations each
///
/// Every package starts in one city and has to be delivered to the next one, so every
/// delivery needs a truck leg, a flight and another truck leg.
pub fn deliveries(
    packages: usize,
    cities: usize,
    locations: usize,
    planes: usize,
) -> (World, WorldTarget) {
    assert!(cities > 1, "a delivery needs at least two cities");
    assert!(locations > 1, "a city needs an airport and a location");

    let mut city_map = Map::new();
    let mut location_city = Map::new();
    let mut trucks = Map::new();
    for city in 0..cities {
        let name = format!("c{city}");
        let locs: List<Loc> = (0..locations).map(|i| location(city, i)).collect();
        for loc in locs.iter() {
            location_city.insert(loc.clone(), name.clone());
        }

        let truck = format!("t{city}");
        // trucks start at the airport of their city
        trucks.insert(truck.clone(), location(city, 0));
        city_map.insert(
            name,
            City {
                airport: location(city, 0),
                locations: locs,
                truck,
            },
        );
    }

    // every plane starts at the airport of the first city
    let planes = (0..planes)
        .map(|p| (format!("p{p}"), location(0, 0)))
        .collect::<Map<String, Loc>>();

    let mut initial = Map::new();
    let mut target = Map::new();
    for package in 0..packages {
        let name = format!("pkg{package:03}");
        let from_city = package % cities;
        let to_city = (package + 1) % cities;
        // never the airport, so that the delivery needs a truck on both ends
        let from = location(from_city, 1 + package % (locations - 1));
        let to = location(to_city, 1 + (package + 1) % (locations - 1));

        initial.insert(name.clone(), PackageLoc::At(from));
        target.insert(name, PackageLoc::At(to));
    }

    (
        World {
            packages: initial,
            trucks,
            planes,
            cities: city_map,
            location_city,
        },
        WorldTarget { packages: target },
    )
}
