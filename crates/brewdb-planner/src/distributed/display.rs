use std::fmt;

use datafusion::physical_plan::{DisplayAs, DisplayFormatType};

use super::fragment::{FragmentInstance, PlanFragment};

impl DisplayAs for PlanFragment {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default => write!(
                f,
                "Fragment: fragment_id={:?}, kind={:?}",
                self.fragment_id, self.kind
            ),
            DisplayFormatType::Verbose | DisplayFormatType::TreeRender => {
                fmt_fragment_details(self, f, 0)
            }
        }
    }
}

impl DisplayAs for FragmentInstance {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default => write!(
                f,
                "FragmentInstance: instance_id={}, fragment_id={:?}, worker_id={}, endpoint={}",
                self.instance_id,
                self.fragment_id(),
                self.worker_id,
                self.endpoint
            ),
            DisplayFormatType::Verbose | DisplayFormatType::TreeRender => {
                writeln!(f, "FragmentInstance")?;
                write_indent(f, 1)?;
                writeln!(f, "instance_id={}", self.instance_id)?;
                write_indent(f, 1)?;
                writeln!(f, "worker_id={}", self.worker_id)?;
                write_indent(f, 1)?;
                writeln!(f, "endpoint={}", self.endpoint)?;
                write_indent(f, 1)?;
                writeln!(f, "fragment_id={:?}", self.fragment_id())?;
                write_indent(f, 1)?;
                fmt_fragment_details(self.fragment(), f, 1)
            }
        }
    }
}

fn fmt_fragment_details(
    fragment: &PlanFragment,
    f: &mut fmt::Formatter<'_>,
    indent: usize,
) -> fmt::Result {
    writeln!(f, "Fragment")?;
    write_indent(f, indent + 1)?;
    writeln!(f, "fragment_id={:?}", fragment.fragment_id)?;
    write_indent(f, indent + 1)?;
    writeln!(f, "kind={:?}", fragment.kind)?;
    if let Some(root) = &fragment.root {
        write_indent(f, indent + 1)?;
        writeln!(f, "root={root:?}")?;
    }
    if let Some(local_plan) = &fragment.local_plan {
        write_indent(f, indent + 1)?;
        writeln!(f, "local_plan={local_plan:?}")?;
    }
    Ok(())
}

fn write_indent(f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
    write!(f, "{:indent$}", "", indent = indent * 2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::distributed::{PlanFragmentId, PlanFragmentKind};
    use datafusion::physical_plan::{DefaultDisplay, VerboseDisplay};

    fn fragment() -> PlanFragment {
        PlanFragment {
            fragment_id: PlanFragmentId(7),
            kind: PlanFragmentKind::Root,
            root: None,
            local_plan: None,
        }
    }

    #[test]
    fn fragment_default_display_shows_identity() {
        let display = format!("{}", DefaultDisplay(fragment()));
        assert!(display.contains("fragment_id=PlanFragmentId(7)"));
        assert!(display.contains("kind=Root"));
    }

    #[test]
    fn fragment_instance_verbose_display_shows_worker_identity() {
        let instance = FragmentInstance::scheduled(
            0,
            fragment(),
            uuid::Uuid::new_v4(),
            "rpc://worker-1",
            vec![],
        );
        let display = format!("{}", VerboseDisplay(instance));
        assert!(display.contains("FragmentInstance"));
        assert!(display.contains("worker_id="));
        assert!(display.contains("endpoint=rpc://worker-1"));
    }
}
