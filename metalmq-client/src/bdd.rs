use std::future::Future;
use std::io::Write;
use std::pin::Pin;
use crate::{ClientError, Result};
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

// TODO group the scenarios to features, how?
type InitFn<W> = fn() -> Pin<Box<dyn Future<Output=Result<W>>>>;
type StepFn<W> = for<'r> fn(&'r mut W) -> Pin<Box<dyn Future<Output=Result<()>> + 'r>>;

#[macro_export]
macro_rules! init {
    ($wtype:ty, { $($body:tt)* }) => {
        || -> ::std::pin::Pin<::std::boxed::Box<dyn ::std::future::Future<Output=$crate::Result<$wtype>>>> {
            ::std::boxed::Box::pin(async move { $($body)* })
        }
    }
}

#[macro_export]
macro_rules! step {
    (|$wname:ident: $wtype:ty| $($body:tt)*) => {
        |$wname: &'_ mut $wtype| -> ::std::pin::Pin<::std::boxed::Box<dyn ::std::future::Future<Output=$crate::Result<()>> + '_>> {
            ::std::boxed::Box::pin(async move { $($body)* })
        }
    }
}

pub enum Step<W> {
    Feature(String),
    Given(String, StepFn<W>),
    When(String, StepFn<W>),
    Then(String, StepFn<W>)
}

pub struct Steps<W> {
    world: W,
    steps: Vec<Step<W>>
}

impl<W> Steps<W> {
    pub async fn feature(text: &str, f: InitFn<W>) -> Self {
        Steps {
            world: f().await.unwrap(),
            steps: vec![Step::Feature(text.to_string())]
        }
    }

    pub fn given(&mut self, text: &str, f: StepFn<W>) -> &mut Self {
        self.steps.push(Step::Given(text.to_string(), f));
        self
    }

    pub fn when(&mut self, text: &str, f: StepFn<W>) -> &mut Self {
        self.steps.push(Step::When(text.to_string(), f));
        self
    }

    pub fn then(&mut self, text: &str, f: StepFn<W>) -> &mut Self {
        self.steps.push(Step::Then(text.to_string(), f));
        self
    }

    pub async fn check(&mut self) {
        use Step::*;

        for step in &self.steps {
            write(&step);

            match step {
                Given(_, f) =>
                    if let Err(e) = f(&mut self.world).await {
                        fail(e);
                    },
                When(_, f) =>
                    if let Err(e) = f(&mut self.world).await {
                        fail(e);
                    },
                Then(_, f) =>
                    if let Err(e) = f(&mut self.world).await {
                        fail(e);
                    },
                _ => ()
            }
        }
    }
}

fn fail(error: Box<dyn std::error::Error>) {
    let mut stdout = StandardStream::stdout(ColorChoice::Always);
    stdout.set_color(ColorSpec::new().set_fg(Some(Color::Red))).unwrap();
    writeln!(&mut stdout, "Step failed with {:?}", error).unwrap();
    stdout.reset().unwrap();

    assert!(false);
}

fn write<W>(step: &Step<W>) {
    use Step::*;

    let mut stdout = StandardStream::stdout(ColorChoice::Always);

    let (color, pre, new_line, indent, text) = match step {
        Feature(text) => (Color::Cyan, "Feature", true, 1, text),
        Given(text, _) => (Color::Yellow, "Given", false, 2, text),
        When(text, _) => (Color::Blue, "When", false, 2, text),
        Then(text, _) => (Color::Green, "Then", false, 2, text)
    };

    if new_line {
        write!(&mut stdout, "\n").unwrap();
    }
    stdout.set_color(ColorSpec::new().set_fg(Some(color))).unwrap();
    write!(&mut stdout, "{}{} ", "  ".repeat(indent), pre).unwrap();

    stdout.reset().unwrap();
    writeln!(&mut stdout, "{}", text).unwrap();
}

pub fn to_client_error<T: std::fmt::Debug>(result: Result<T>) -> ClientError {
    *(result.unwrap_err().downcast::<ClientError>().unwrap())
}
