use super::System;

pub struct SystemWriter {
    write: Box<dyn FnOnce(&mut System)>,
}

impl SystemWriter {
    pub fn new<F: FnOnce(&mut System) + 'static>(fun: F) -> Self {
        Self {
            write: Box::new(fun),
        }
    }

    pub fn write(self, system: &mut System) {
        (self.write)(system);
    }
}

pub trait IntoSystemWriter {
    fn into_system_writer(self) -> SystemWriter;
}
