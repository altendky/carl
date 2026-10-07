"""Audited, explicit route revisions before removing configuration overrides."""

from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter_ns, time_ns
from uuid import uuid4

import anyio

from carl.core.components import Component, ComponentId
from carl.core.marketplace_search import FacebookSearchTargetSpecification
from carl.core.models import JsonValue
from carl.core.review_workspace import ReviseWorkspaceSearchTrackRequest
from carl.io.provenance import collect_code_provenance_async, process_invocation
from carl.io.sqlite import Database
from carl.review import ReviewApplication


async def cutover_route_overrides(
    database: Database,
    repository_root: Path,
    replacements: dict[tuple[str, ...], tuple[str, ...]],
) -> dict[str, JsonValue]:
    """Revise unfinished work and current track specs without acquiring evidence.

    Removing the configuration overrides is a separate final action: if a lease,
    deduplication collision or concurrent track revision prevents this operation,
    the caller must leave the configuration intact and resolve that conflict.
    """

    operation_identifier = str(uuid4())
    audit_identifier = str(uuid4())
    started = perf_counter_ns()
    count = 0
    provenance = await collect_code_provenance_async(repository_root)
    with anyio.CancelScope(shield=True):
        await database.begin_operation(
            operation_id=operation_identifier,
            component=Component(
                identifier=ComponentId(("carl", "work", "cutover_routes")),
                output_schema_version=1,
                implementation=cutover_route_overrides,
            ),
            provenance=provenance,
            invocation=process_invocation(),
            configuration={
                "replacements": [
                    {"previous_network_path": list(source), "network_path": list(target)}
                    for source, target in replacements.items()
                ],
            },
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        try:
            count = await database.cutover_work_routes(
                replacements=replacements,
                operation_identifier=operation_identifier,
                audit_record_identifier=audit_identifier,
                at_utc_ns=time_ns(),
                started_monotonic_ns=started,
            )
        except BaseException as error:
            await database.fail_operation(
                operation_id=operation_identifier,
                error={
                    "kind": "route_cutover_failure",
                    "type": type(error).__name__,
                    "message": str(error),
                },
                result={"state": "failed"},
                ended_at_utc=datetime.now(UTC).isoformat(),
                duration_ns=perf_counter_ns() - started,
            )
            raise
    application = ReviewApplication(database=database, repository_root=repository_root)
    tracks: list[JsonValue] = []
    for workspace in await application.list_review_workspaces(include_archived=True):
        for track in workspace.search_tracks:
            specification = track.search_specification
            if not isinstance(specification, FacebookSearchTargetSpecification):
                continue
            target = replacements.get(specification.search.requested_network_path)
            if target is None:
                continue
            revised = await application.revise_workspace_search_track(
                ReviseWorkspaceSearchTrackRequest(
                    workspace_record_identifier=workspace.record_identifier,
                    track_identifier=track.track_identifier,
                    expected_version=track.search_specification_version,
                    search=specification.model_copy(
                        update={
                            "search": specification.search.model_copy(
                                update={
                                    "network_path": target,
                                    "proton_route": None,
                                }
                            ),
                        }
                    ),
                )
            )
            tracks.append(
                {
                    "workspace_record_identifier": workspace.record_identifier,
                    "track_identifier": track.track_identifier,
                    "search_specification_version": revised.search_specification_version,
                }
            )
    return {
        "operation_identifier": operation_identifier,
        "audit_record_identifier": audit_identifier,
        "revised_work_count": count,
        "revised_track_count": len(tracks),
        "tracks": tracks,
    }
