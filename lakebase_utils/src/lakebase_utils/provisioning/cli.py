"""``lakebase-provision`` CLI — validate / plan / apply subcommands."""

from __future__ import annotations

import argparse
import logging
import sys

from .config import ConfigError, load_config
from .engine import ProvisioningEngine, format_plan

EXIT_OK = 0
EXIT_INVALID_CONFIG = 2
EXIT_PARTIAL = 1


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="lakebase-provision",
        description="Data-layer provisioning for Lakebase Autoscaling "
                    "(roles, GRANTs, Data API exposed schemas).",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("-f", "--file", required=True,
                        help="Path to provisioning YAML config")
    common.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    sub.add_parser("validate", parents=[common],
                   help="Schema-validate the config (no network)")
    sub.add_parser("plan", parents=[common],
                   help="Print the actions that would be applied (no writes)")

    apply_p = sub.add_parser("apply", parents=[common],
                             help="Apply the plan to Lakebase")
    apply_p.add_argument("--auto-approve", action="store_true",
                         help="Skip interactive confirmation")

    return p


def _confirm() -> bool:
    try:
        return input("Apply these changes? [y/N] ").strip().lower() == "y"
    except EOFError:
        return False


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(level=args.log_level, format="%(levelname)s %(name)s: %(message)s")

    try:
        config = load_config(args.file)
    except ConfigError as e:
        print(f"error: {e}", file=sys.stderr)
        return EXIT_INVALID_CONFIG

    if args.cmd == "validate":
        print(f"OK: {args.file} is a valid Lakebase provisioning config.")
        return EXIT_OK

    engine = ProvisioningEngine(config)
    try:
        plan = engine.plan()
        print(format_plan(plan))

        if args.cmd == "plan":
            return EXIT_OK

        # apply
        if plan.is_empty():
            return EXIT_OK
        if not args.auto_approve and not _confirm():
            print("Aborted.")
            return EXIT_OK
        engine.apply(dry_run=False, auto_approve=True)
        print("\nApplied successfully.")
        return EXIT_OK
    finally:
        engine.close()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
