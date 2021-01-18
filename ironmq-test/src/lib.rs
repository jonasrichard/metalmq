use std::future::Future;
use std::io::Write;
use std::pin::Pin;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

// TODO future should return with Result<(), Box<dyn std::error::Error>>
// TODO check should check if the return value is Err or Ok
type StepFn<W> = for<'r> fn(&'r mut W) -> Pin<Box<dyn Future<Output=()> + 'r>>;

#[macro_export]
macro_rules! step {
    (|$wname:ident: $wtype:ty| $($body:tt)*) => {
        |$wname: &'_ mut $wtype| -> ::std::pin::Pin<::std::boxed::Box<dyn ::std::future::Future<Output=()> + '_>> {
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

impl<W: Default> Steps<W> {
    pub fn new() -> Self {
        Steps {
            world: W::default(),
            steps: vec![]
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

    pub async fn check(&mut self) {
        use Step::*;

        for step in &self.steps {
            write(&step);

            match step {
                Given(_, f) => f(&mut self.world).await,
                _ => ()
            }
        }
    }
}

fn write<W>(step: &Step<W>) {
    use Step::*;

    let mut stdout = StandardStream::stdout(ColorChoice::Always);

    let (color, pre, indent, text) = match step {
        Feature(text) => (Color::Cyan, "Feature", 0, text),
        Given(text, _) => (Color::Yellow, "Given", 1, text),
        When(text, _) => (Color::Blue, "When", 1, text),
        Then(text, _) => (Color::Green, "Then", 1, text)
    };

    stdout.set_color(ColorSpec::new().set_fg(Some(color))).unwrap();
    write!(&mut stdout, "{}{} ", "  ".repeat(indent), pre).unwrap();

    stdout.reset().unwrap();
    writeln!(&mut stdout, "{}", text).unwrap();
}
