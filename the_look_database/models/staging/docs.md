{% docs order_item_status %}
    
The current state of the order. It takes one of the following values: 

| status         | definition                                       |
|----------------|--------------------------------------------------|
| Processing     | Order placed, not yet shipped                    |
| Shipped        | Order has been shipped, not yet been delivered   |
| Complete       | Order has been received by customers             |
| Cancelled      | Customer cancelled the order                     |
| Returned       | Item has been returned                           |

{% enddocs %}



{% docs traffic_source_description %}

This column represents the marketing channel that drove the user to the site for a specific event:

* **Adwords**: Paid search traffic specifically from Google Ads (formerly AdWords).
* **YouTube**: Traffic originating from video content or video-based advertisements.
* **Facebook**: Traffic from social media campaigns or organic social shares.
* **Organic**: Free traffic from search engine results.
* **Email**: Conversion driven by direct email outreach.

{% enddocs %}



{% docs user_traffic_source_description %}

This column identifies the **first touchpoint** that led to a user's account creation:

* **Facebook**: Paid or organic social traffic.
* **Search**: Paid search engine marketing (PPC).
* **Organic**: Unpaid search engine results.
* **Display**: Digital banner/display advertisements.
* **Email**: Email marketing campaigns.

{% enddocs %}



{% docs event_type_description %}

Describes the user's action during a web session. These are typically analyzed in sequence to measure conversion funnels:

1. **home**: Landing on the main store page.
2. **department**: Browsing a specific category (e.g., Active, Accessories).
3. **product**: Viewing a single item's details.
4. **cart**: Adding an item to the basket (indicates high intent).
5. **purchase**: The final checkout event.
6. **cancel**: A reversal of a previous purchase.

{% enddocs %}