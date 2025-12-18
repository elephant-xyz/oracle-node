import type { StepAggregateItem } from "shared/types.js";
import { queryAllStepAggregates } from "shared/repository.js";

/**
 * Event structure for CloudWatch custom widget Lambda invocation.
 */
interface CloudWatchCustomWidgetEvent {
  /** If true, return widget documentation in Markdown format. */
  describe?: boolean;
  /** Widget context containing dashboard and widget metadata. */
  widgetContext?: {
    dashboardName: string;
    widgetId: string;
    accountId: string;
    height: number;
    width: number;
    params?: Record<string, unknown>;
  };
}

/**
 * Aggregated counts for a single phase.
 */
interface PhaseAggregate {
  /** Phase name (e.g., "prepare", "transform"). */
  phase: string;
  /** Total count of executions in progress (includes SCHEDULED and PARKED). */
  inProgressCount: number;
  /** Total count of failed executions. */
  failedCount: number;
  /** Total count of succeeded executions. */
  succeededCount: number;
  /** Latest update timestamp across all aggregates for this phase. */
  lastUpdated: string | null;
}

/**
 * Returns widget documentation in Markdown format.
 * @returns JSON string containing markdown documentation
 */
const getDocumentation = (): string => {
  return JSON.stringify({
    markdown: `## Workflow Phase Aggregates

This widget displays current snapshot counts of workflow executions by phase, aggregated from the DynamoDB workflow-state table.

### Displayed Columns

- **Phase**: Workflow phase name (e.g., Prepare, Transform, SVL)
- **IN_PROGRESS**: Current count of executions in progress (includes SCHEDULED and PARKED states)
- **FAILED**: Current count of failed executions
- **SUCCEEDED**: Current count of succeeded executions
- **TOTAL**: Sum of all execution counts for the phase
- **Last Updated**: Most recent update timestamp across all aggregates for the phase

### Data Source

Data is queried from the \`workflow-state\` DynamoDB table using step aggregates. These aggregates are updated in real-time as workflow events are processed.

### Note

This widget shows **current snapshot counts**, not historical totals. The counts reflect the current state of executions in each phase, not cumulative totals since the beginning of time.`,
  });
};

/**
 * Aggregates step aggregates by phase, summing counts and finding the latest update time.
 * @param aggregates - Array of step aggregate items from DynamoDB
 * @returns Map of phase name to aggregated phase data
 */
const aggregateByPhase = (
  aggregates: StepAggregateItem[],
): Map<string, PhaseAggregate> => {
  const phaseMap = new Map<string, PhaseAggregate>();

  for (const agg of aggregates) {
    const existing = phaseMap.get(agg.phase);

    if (existing) {
      existing.inProgressCount += agg.inProgressCount;
      existing.failedCount += agg.failedCount;
      existing.succeededCount += agg.succeededCount;

      // Update lastUpdated if this aggregate is newer
      if (!existing.lastUpdated || agg.updatedAt > existing.lastUpdated) {
        existing.lastUpdated = agg.updatedAt;
      }
    } else {
      phaseMap.set(agg.phase, {
        phase: agg.phase,
        inProgressCount: agg.inProgressCount,
        failedCount: agg.failedCount,
        succeededCount: agg.succeededCount,
        lastUpdated: agg.updatedAt,
      });
    }
  }

  return phaseMap;
};

/**
 * Escapes HTML special characters to prevent XSS.
 * @param str - String to escape
 * @returns HTML-escaped string
 */
const escapeHtml = (str: string): string => {
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
};

/**
 * Formats an ISO timestamp for display.
 * @param isoDate - ISO timestamp string
 * @returns Formatted date string
 */
const formatDate = (isoDate: string | null): string => {
  if (!isoDate) {
    return "N/A";
  }

  try {
    const date = new Date(isoDate);
    return date.toLocaleString("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  } catch {
    return isoDate;
  }
};

/**
 * Capitalizes the first letter of a string.
 * @param str - String to capitalize
 * @returns Capitalized string
 */
const capitalize = (str: string): string => {
  if (str.length === 0) {
    return str;
  }
  return str.charAt(0).toUpperCase() + str.slice(1);
};

/**
 * Generates HTML table displaying phase aggregates.
 * @param phaseAggregates - Map of phase aggregates to display
 * @returns HTML string for the table
 */
const generateHtml = (phaseAggregates: Map<string, PhaseAggregate>): string => {
  if (phaseAggregates.size === 0) {
    return `
      <p>No phase aggregates found. The workflow-state table may be empty or no executions have been processed yet.</p>
    `;
  }

  // Sort phases alphabetically for consistent display
  const sortedPhases = Array.from(phaseAggregates.values()).sort((a, b) =>
    a.phase.localeCompare(b.phase),
  );

  const rows = sortedPhases
    .map((phaseAgg) => {
      const total =
        phaseAgg.inProgressCount +
        phaseAgg.failedCount +
        phaseAgg.succeededCount;

      return `
      <tr>
        <td><strong>${escapeHtml(capitalize(phaseAgg.phase))}</strong></td>
        <td style="text-align: right;">${phaseAgg.inProgressCount}</td>
        <td style="text-align: right;">${phaseAgg.failedCount}</td>
        <td style="text-align: right;">${phaseAgg.succeededCount}</td>
        <td style="text-align: right;"><strong>${total}</strong></td>
        <td>${formatDate(phaseAgg.lastUpdated)}</td>
      </tr>
    `;
    })
    .join("");

  return `
    <style>
      table {
        width: 100%;
        border-collapse: collapse;
      }
      th {
        background-color: #f5f5f5;
        padding: 8px 12px;
        text-align: left;
        border-bottom: 2px solid #ddd;
        font-weight: 600;
      }
      td {
        padding: 8px 12px;
        border-bottom: 1px solid #eee;
      }
      tr:hover {
        background-color: #f9f9f9;
      }
    </style>
    <table>
      <thead>
        <tr>
          <th>Phase</th>
          <th style="text-align: right;">IN_PROGRESS</th>
          <th style="text-align: right;">FAILED</th>
          <th style="text-align: right;">SUCCEEDED</th>
          <th style="text-align: right;">TOTAL</th>
          <th>Last Updated</th>
        </tr>
      </thead>
      <tbody>
        ${rows}
      </tbody>
    </table>
  `;
};

/**
 * CloudWatch custom widget handler.
 * Queries DynamoDB workflow-state table for step aggregates and displays them
 * aggregated by phase in an HTML table.
 *
 * @param event - CloudWatch custom widget event
 * @returns HTML string to display in the dashboard widget
 */
export const handler = async (
  event: CloudWatchCustomWidgetEvent,
): Promise<string> => {
  console.info("dashboard-state-aggregates-widget-invoked", { event });

  if (event.describe) {
    return getDocumentation();
  }

  try {
    // Query all step aggregates from DynamoDB
    // We fetch all aggregates without filtering to get a complete picture
    const result = await queryAllStepAggregates({
      limit: 1000, // Reasonable limit for dashboard display
    });

    console.info("aggregates-queried", {
      count: result.items.length,
      hasMore: result.hasMore,
    });

    // Aggregate by phase
    const phaseAggregates = aggregateByPhase(result.items);

    console.info("phases-aggregated", {
      phaseCount: phaseAggregates.size,
      phases: Array.from(phaseAggregates.keys()),
    });

    return generateHtml(phaseAggregates);
  } catch (error) {
    console.error("widget-error", {
      error: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : undefined,
    });

    return `
      <h3>Error loading data</h3>
      <p>${escapeHtml(error instanceof Error ? error.message : String(error))}</p>
    `;
  }
};
