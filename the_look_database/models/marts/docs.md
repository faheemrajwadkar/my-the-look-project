{% docs order_status_desc %}

The raw status of the order as captured in the source system.

| Status | Definition |
| :--- | :--- |
| **Processing** | The order has been placed but not yet fulfilled or shipped. |
| **Shipped** | The order has left the distribution center and is in transit. |
| **Complete** | The order was successfully delivered to the customer and not returned. |
| **Cancelled** | The order was voided by the customer or system before shipping. |
| **Returned** | The customer received the order but initiated a return process. |

{% enddocs %}


{% docs order_derived_status_desc %}

A business-logic field used to determine the **financial health** of an order based on profit margins.

### Logic:
An order is considered **'Successful'** if the **Net Revenue** (Gross Revenue minus Returns) is at least **20% higher** than the **Procurement Cost** of the items.

* **Successful**: `Net Revenue > 1.2 * Total Cost`
* **Failed**: `Net Revenue <= 1.2 * Total Cost` (Includes low-margin sales, cancellations, and full returns)

**Purpose**: This helps stakeholders quickly filter out orders that did not meet the target profitability threshold due to heavy discounting or high return costs.

{% enddocs %}


{% docs order_item_status_desc %}

The raw status of the order item as captured in the source system.

| Status | Definition |
| :--- | :--- |
| **Processing** | The order has been placed but not yet fulfilled or shipped. |
| **Shipped** | The order has left the distribution center and is in transit. |
| **Complete** | The order was successfully delivered to the customer and not returned. |
| **Cancelled** | The order was voided by the customer or system before shipping. |
| **Returned** | The customer received the order but initiated a return process. |

{% enddocs %}


{% docs session_result_desc %}

The final outcome or "Success Level" of a user session, determined by a hierarchical logic flow.

### Outcome Definitions (In Priority Order):

| Result | Logic / Definition |
| :--- | :--- |
| **purchase_completed** | The highest value outcome. Recorded if at least one purchase occurred during the session. |
| **cart_abandoned** | High intent, no conversion. The user added items to their cart or visited the cart page but did not complete a purchase. |
| **cancelled** | User-terminated. The session ended specifically with a 'cancel' event (e.g., closing a modal or clicking a cancel button). |
| **abandoned** | Low intent / Bounce. The user left the site without purchasing, without abandoned items in the cart, and without an explicit cancel event. |

{% enddocs %}