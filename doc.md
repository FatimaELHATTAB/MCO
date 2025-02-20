# Cluster ID (`cluster_id`) - Calculation & Importance

## **1️⃣ What is a Cluster ID?**
A **Cluster ID (`cluster_id`)** is a unique identifier assigned to a group of entities (`intern_id`, `target_id`) that are connected by a matching rule.  
It ensures that entities related to the same target remain grouped together.

---

## **2️⃣ How is the Cluster ID Calculated?**
The `cluster_id` is computed using a **graph-based approach**:

1. **Filter Active Rows**  
   - Consider only rows where `matching_end_date IS NULL` (i.e., still valid matches).

2. **Build a Connection Graph**  
   - Each `intern_id` is connected to its respective `target_id`.
   - If multiple `intern_id`s are linked to the same `target_id`, they are grouped together.

3. **Find Connected Components**  
   - Using **Depth-First Search (DFS)**, we traverse all connected nodes (`intern_id`, `target_id`).
   - Each group of connected entities forms a **cluster**.

4. **Generate a Unique Cluster ID**  
   - A unique identifier (`cluster_id`) is generated using a **SHA-256 hash** of all `intern_id`s in a cluster and its associated `target_name`.

5. **Mark Duplicates (`is_duplicate`)**  
   - If a `cluster_id` appears in **multiple active rows**, all rows in that cluster are marked as **`is_duplicate = True`**.

---

## **3️⃣ Why is the Cluster ID Important?**
The **Cluster ID** is essential for maintaining data consistency and avoiding mismatches.  
### 🔹 **Key Benefits:**
✅ **Ensures entity relationships stay consistent** across multiple updates.  
✅ **Prevents duplicate entries** by marking clusters with multiple connections.  
✅ **Helps in tracking changes** (if a new `intern_id` appears, old ones are marked `INTERN_CHANGED`).  
✅ **Improves database performance** by reducing redundant computations on entity matching.  
✅ **Facilitates cluster-based operations**, such as bulk updates, analytics, and audits.

---

## **4️⃣ Example of Cluster Calculation**
### **🔹 Input Data**
| intern_id | target_id | target_name | matching_end_date |
|-----------|----------|-------------|-------------------|
| A         | 1        | Company X   | NULL              |
| B         | 1        | Company X   | NULL              |
| C         | 2        | Company Y   | NULL              |
| D         | 3        | Company Z   | NULL              |

### **🔹 Generated Cluster IDs**
| intern_id | target_id | cluster_id                                  | is_duplicate |
|-----------|----------|----------------------------------------------|-------------|
| A         | 1        | `hash(A,B,Company X)`                       | True        |
| B         | 1        | `hash(A,B,Company X)`                       | True        |
| C         | 2        | `hash(C,Company Y)`                         | False       |
| D         | 3        | `hash(D,Company Z)`                         | False       |

📌 **In this example:**
- `A` and `B` belong to the same cluster (`Company X`), so they get the same `cluster_id` and are marked as **duplicates**.
- `C` and `D` are unique, so they get separate `cluster_id`s and are **not duplicates**.

---

## **5️⃣ Summary**
- **Cluster ID** is assigned using a **graph-based method** with **DFS traversal**.
- **Ensures integrity** by keeping related entities together.
- **Helps detect duplicates**, improving **data quality** and **reducing redundancy**.
- **Prevents overwriting of historical data**, ensuring traceability.

🚀 **By using `cluster_id`, we ensure that related entities stay grouped while detecting meaningful changes in data over time!**
