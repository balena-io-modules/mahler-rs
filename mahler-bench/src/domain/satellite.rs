//! Satellite: the observation scheduling domain
//!
//! Each satellite carries instruments and has a list of images to acquire. Taking an
//! image means powering up the right instrument, calibrating it against its calibration
//! direction, turning the satellite towards the image direction and shooting. Only one
//! instrument per satellite can be powered at a time. This is the AIPS-2002 `Satellite`
//! domain, one of the domains
//! [SHOP2](https://www.cs.umd.edu/~nau/papers/nau2003shop2.pdf) was evaluated on.
//!
//! Unlike the other two domains, every job here is *scoped*: the handlers only use
//! `View`, `Target`, `RawTarget` and `Args`, all relative to their own path under
//! `/satellites/{satellite}`. Satellites therefore never conflict with each other and
//! the planner merges their work into concurrent branches, which makes this the domain
//! that exercises candidate merging and parallel workflow execution.
//!
//! Within a satellite, the path of each action covers all the state its precondition
//! reads, so the planner only runs two steps of one satellite in parallel when neither
//! depends on the other. Turning towards the calibration direction can happen while
//! instruments are switched, but calibrating waits for both, and taking an image waits
//! for the calibration and the turn towards the image.

use mahler::extract::{Args, RawTarget, Target, View};
use mahler::job::{none, update};
use mahler::state::{Map, State};
use mahler::task::{Handler, Task};
use mahler::worker::{Ready, Worker};
use serde::{Deserialize, Serialize};

/// An instrument, with the direction it has to point at to be calibrated
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct Instrument {
    pub power_on: bool,
    pub calibrated: bool,
    pub calibration_direction: String,
}

impl State for Instrument {
    type Target = Self;
}

/// An image to acquire, with the instrument and direction it needs
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct Image {
    pub taken: bool,
    pub direction: String,
    pub instrument: String,
}

/// Only `taken` is a goal: the direction and the instrument describe the image, they
/// are not something a plan can change
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct ImageTarget {
    pub taken: bool,
}

impl State for Image {
    type Target = ImageTarget;
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Satellite {
    pub pointing: String,
    pub instruments: Map<String, Instrument>,
    pub images: Map<String, Image>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SatelliteTarget {
    pub images: Map<String, ImageTarget>,
}

impl State for Satellite {
    type Target = SatelliteTarget;
}

#[derive(Serialize, Deserialize, Debug)]
pub struct World {
    pub satellites: Map<String, Satellite>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorldTarget {
    pub satellites: Map<String, SatelliteTarget>,
}

impl State for World {
    type Target = WorldTarget;
}

/// Turn the satellite towards the direction given as target
fn turn_to(mut pointing: View<String>, Target(tgt): Target<String>) -> Option<View<String>> {
    if *pointing == tgt {
        return None;
    }

    *pointing = tgt;
    Some(pointing)
}

/// Power up the instrument given as target. A newly powered instrument needs calibration.
///
/// The view is every instrument of the satellite, since only one can be powered at a time.
fn switch_on(
    mut instruments: View<Map<String, Instrument>>,
    RawTarget(name): RawTarget<String>,
) -> Option<View<Map<String, Instrument>>> {
    if instruments.iter().any(|(_, i)| i.power_on) {
        return None;
    }

    let instrument = instruments.get_mut(&name)?;
    instrument.power_on = true;
    instrument.calibrated = false;
    Some(instruments)
}

/// Power an instrument down to free the satellite power for another one
fn switch_off(mut instrument: View<Instrument>) -> Option<View<Instrument>> {
    if !instrument.power_on {
        return None;
    }

    instrument.power_on = false;
    instrument.calibrated = false;
    Some(instrument)
}

/// Calibrate the powered instrument given as target, with the satellite pointing at its
/// calibration direction
fn calibrate(
    mut sat: View<Satellite>,
    RawTarget(name): RawTarget<String>,
) -> Option<View<Satellite>> {
    let pointing = sat.pointing.clone();
    let instrument = sat.instruments.get_mut(&name)?;
    if !instrument.power_on || instrument.calibrated || instrument.calibration_direction != pointing
    {
        return None;
    }

    instrument.calibrated = true;
    Some(sat)
}

/// Acquire the image given as target, with its instrument calibrated and the satellite
/// pointing at the image direction
fn take_image(
    mut sat: View<Satellite>,
    RawTarget(name): RawTarget<String>,
) -> Option<View<Satellite>> {
    let image = sat.images.get(&name)?;
    let ready = sat
        .instruments
        .get(&image.instrument)
        .map(|i| i.power_on && i.calibrated)
        .unwrap_or(false);
    if image.taken || !ready || image.direction != sat.pointing {
        return None;
    }

    sat.images.get_mut(&name)?.taken = true;
    Some(sat)
}

/// Turn the satellite from `from` to `to`, unless it already points there
fn turn(from: &String, to: &String) -> Option<Task> {
    (from != to).then(|| turn_to.with_target(to.clone()))
}

/// Plan the acquisition of the next pending image of a satellite
///
/// Images that can use the instrument that is already powered and calibrated are
/// preferred, so a satellite does not power instruments up and down more than it needs.
fn observe(sat: View<Satellite>, Target(tgt): Target<Satellite>) -> Vec<Task> {
    let pending = |name: &String| -> bool {
        tgt.images.get(name).map(|i| i.taken).unwrap_or(false)
            && !sat.images.get(name).map(|i| i.taken).unwrap_or(true)
    };

    let ready = |image: &Image| -> bool {
        sat.instruments
            .get(&image.instrument)
            .map(|i| i.power_on && i.calibrated)
            .unwrap_or(false)
    };

    let next = sat
        .images
        .iter()
        .filter(|(name, _)| pending(name))
        // an image that needs no instrument switch comes first
        .min_by_key(|(_, image)| !ready(image));

    let Some((name, image)) = next else {
        return Vec::new();
    };

    let Some(instrument) = sat.instruments.get(&image.instrument) else {
        return Vec::new();
    };

    let mut tasks = Vec::new();
    let mut pointing = &sat.pointing;

    if !instrument.power_on || !instrument.calibrated {
        // the turn comes first, so it can run while the instruments are switched
        tasks.extend(turn(pointing, &instrument.calibration_direction));
        pointing = &instrument.calibration_direction;
    }

    if !instrument.power_on {
        // only one instrument can be powered at a time
        for (other, state) in sat.instruments.iter() {
            if state.power_on {
                tasks.push(switch_off.with_arg("instrument", other.clone()));
            }
        }

        tasks.push(switch_on.with_target(image.instrument.clone()));
    }

    if !instrument.power_on || !instrument.calibrated {
        tasks.push(calibrate.with_target(image.instrument.clone()));
    }

    tasks.extend(turn(pointing, &image.direction));
    tasks.push(take_image.with_target(name.clone()));

    tasks
}

pub fn worker(initial: World) -> Worker<World, Ready> {
    Worker::new()
        .job("/satellites/{satellite}", update(observe))
        .job(
            "/satellites/{satellite}/pointing",
            none(turn_to).with_description(
                |Args(satellite): Args<String>, Target(to): Target<String>| {
                    format!("turn {satellite} to {to}")
                },
            ),
        )
        .jobs(
            "/satellites/{satellite}",
            [
                none(calibrate).with_description(
                    |Args(satellite): Args<String>, RawTarget(instrument): RawTarget<String>| {
                        format!("calibrate {satellite} {instrument}")
                    },
                ),
                none(take_image).with_description(
                    |Args(satellite): Args<String>, RawTarget(image): RawTarget<String>| {
                        format!("take image {image} with {satellite}")
                    },
                ),
            ],
        )
        .job(
            "/satellites/{satellite}/instruments",
            none(switch_on).with_description(
                |Args(satellite): Args<String>, RawTarget(instrument): RawTarget<String>| {
                    format!("switch on {satellite} {instrument}")
                },
            ),
        )
        .job(
            "/satellites/{satellite}/instruments/{instrument}",
            none(switch_off).with_description(
                |Args((satellite, instrument)): Args<(String, String)>| {
                    format!("switch off {satellite} {instrument}")
                },
            ),
        )
        .initial_state(initial)
        .expect("satellite initial state should be valid")
}

/// `satellites` satellites, each with `instruments` instruments and `images` images
/// to acquire
///
/// The images are spread over the instruments and named so that the ones sharing an
/// instrument are acquired one after the other. Every satellite starts pointing at its
/// own parking direction with every instrument powered down, so each one has the same
/// amount of work to do and all of it is independent.
pub fn observations(satellites: usize, instruments: usize, images: usize) -> (World, WorldTarget) {
    let mut initial = Map::new();
    let mut target = Map::new();

    for sat in 0..satellites {
        let name = format!("s{sat}");

        let instrument_map = (0..instruments)
            .map(|i| {
                (
                    format!("i{i}"),
                    Instrument {
                        power_on: false,
                        calibrated: false,
                        calibration_direction: format!("cal-{name}-i{i}"),
                    },
                )
            })
            .collect::<Map<String, Instrument>>();

        let mut image_map = Map::new();
        let mut image_target = Map::new();
        for img in 0..images {
            let instrument = img % instruments;
            // the instrument leads the name so that images sharing one are contiguous
            let image_name = format!("i{instrument}-img{img:03}");
            image_map.insert(
                image_name.clone(),
                Image {
                    taken: false,
                    direction: format!("dir-{name}-{img}"),
                    instrument: format!("i{instrument}"),
                },
            );
            image_target.insert(image_name, ImageTarget { taken: true });
        }

        initial.insert(
            name.clone(),
            Satellite {
                pointing: format!("park-{name}"),
                instruments: instrument_map,
                images: image_map,
            },
        );
        target.insert(
            name,
            SatelliteTarget {
                images: image_target,
            },
        );
    }

    (
        World {
            satellites: initial,
        },
        WorldTarget { satellites: target },
    )
}
