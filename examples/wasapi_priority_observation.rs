//! Silent-output / discarded-input check of the fork's typed worker observation.
#![forbid(unsafe_code)]

#[cfg(target_os = "windows")]
mod check {
    use cpal::{
        platform::{StreamInner, WasapiThreadPriorityObservation, WasapiThreadPriorityStatus},
        traits::{DeviceTrait, HostTrait, StreamTrait},
    };
    use std::{
        error::Error,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, OnceLock,
        },
        time::{Duration, Instant},
    };

    #[derive(Default)]
    struct Counts {
        observation: OnceLock<WasapiThreadPriorityObservation>,
        callbacks: AtomicUsize,
        wrong_status: AtomicUsize,
        errors: AtomicUsize,
    }

    fn stream<T: cpal::SizedSample>(
        device: &cpal::Device,
        config: &cpal::StreamConfig,
        input: bool,
        counts: Arc<Counts>,
    ) -> Result<cpal::Stream, cpal::BuildStreamError> {
        let error_counts = Arc::clone(&counts);
        let observe = move || {
            let expected = if cfg!(feature = "audio_thread_priority") {
                WasapiThreadPriorityStatus::Promoted
            } else {
                WasapiThreadPriorityStatus::Unobserved
            };
            if counts.observation.get().map(|o| o.status()) != Some(expected) {
                counts.wrong_status.fetch_add(1, Ordering::Relaxed);
            }
            counts.callbacks.fetch_add(1, Ordering::Release);
        };
        let on_error = move |_| {
            error_counts.errors.fetch_add(1, Ordering::Relaxed);
        };
        if input {
            device.build_input_stream(config, move |_: &[T], _| observe(), on_error, None)
        } else {
            device.build_output_stream(
                config,
                move |out: &mut [T], _| {
                    out.fill(T::EQUILIBRIUM);
                    observe();
                },
                on_error,
                None,
            )
        }
    }

    pub fn run() -> Result<(), Box<dyn Error>> {
        let host = cpal::default_host();
        if host.id().name() != "WASAPI" {
            return Err("requires default WASAPI host".into());
        }
        for input in [false, true] {
            let device = if input {
                host.default_input_device()
            } else {
                host.default_output_device()
            }
            .ok_or("missing default device")?;
            let supported = if input {
                device.default_input_config()?
            } else {
                device.default_output_config()?
            };
            let config = supported.config();
            for generation in 1..=3 {
                let counts = Arc::new(Counts::default());
                let stream = match supported.sample_format() {
                    cpal::SampleFormat::F32 => {
                        stream::<f32>(&device, &config, input, Arc::clone(&counts))
                    }
                    cpal::SampleFormat::I16 => {
                        stream::<i16>(&device, &config, input, Arc::clone(&counts))
                    }
                    cpal::SampleFormat::I32 => {
                        stream::<i32>(&device, &config, input, Arc::clone(&counts))
                    }
                    _ => return Err("unsupported diagnostic format".into()),
                }?;
                let observation = match stream.as_inner() {
                    StreamInner::Wasapi(wasapi) => wasapi.thread_priority_observation(),
                    #[cfg(feature = "asio")]
                    StreamInner::Asio(_) => return Err("expected WASAPI stream".into()),
                };
                if counts.observation.set(observation.clone()).is_err() {
                    return Err("observation already bound".into());
                }
                stream.play()?;
                let deadline = Instant::now() + Duration::from_secs(5);
                while counts.callbacks.load(Ordering::Acquire) < 32 && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(10));
                }
                drop(stream);
                let callbacks = counts.callbacks.load(Ordering::Relaxed);
                let wrong = counts.wrong_status.load(Ordering::Relaxed);
                let errors = counts.errors.load(Ordering::Relaxed);
                let status = observation.status();
                println!("input={input} generation={generation} callbacks={callbacks} wrong_status={wrong} errors={errors} after_close={status:?}");
                let expected = if cfg!(feature = "audio_thread_priority") {
                    WasapiThreadPriorityStatus::Released
                } else {
                    WasapiThreadPriorityStatus::Unobserved
                };
                if callbacks < 32 || wrong != 0 || errors != 0 || status != expected {
                    return Err("typed worker observation contract failed".into());
                }
            }
        }
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(target_os = "windows")]
    return check::run();
    #[cfg(not(target_os = "windows"))]
    Err("this diagnostic requires WASAPI".into())
}
