

document.addEventListener("DOMContentLoaded", function() {
  fetch("/health")
    .then(response => response.json())
    .then(apiResponse => {
      // Aggregation mode: 'friday' or 'rolling'
      let aggregationMode = 'friday';

      // Initial toggled metrics (visible)
      let toggledKeys = [
        "study_engineering_and_ml",
        "readiness_score",
        "sleep_score",
        "language_learning",
        "coding",
        "read",
        "job_activities"
      ];
      const fixedColors = {
        coding: "#444444",
        language_learning: "#DC143C",
        job_activities: "#008080",
        read: "#FFA500",
        study_engineering_and_ml: "#1E90FF"
      };
      const wellnessKeys = (Array.isArray(apiResponse.metrics_view) && apiResponse.metrics_view.length > 0 && apiResponse.metrics_view[0].wellness)
        ? Object.keys(apiResponse.metrics_view[0].wellness)
        : [];
      const productivityKeys = (Array.isArray(apiResponse.metrics_view) && apiResponse.metrics_view.length > 0)
        ? [...new Set(apiResponse.metrics_view.flatMap(d => d && d.productivity ? Object.keys(d.productivity) : []))]
        : [];

      // Helper to get week number (Friday start)
      function getWeekStartFriday(dateStr) {
        const d = new Date(dateStr);
        // Find previous Friday (or today if Friday)
        const day = d.getDay();
        const diff = (day >= 5) ? day - 5 : day + 2;
        const friday = new Date(d);
        friday.setDate(d.getDate() - diff);
        friday.setHours(0,0,0,0);
        return friday.toISOString().slice(0,10);
      }

      // Helper to get rolling week ending today
      function getRollingWeekEnd(dateStr) {
        const d = new Date(dateStr);
        d.setHours(0,0,0,0);
        return d.toISOString().slice(0,10);
      }

      // Aggregate metrics by week
      function aggregateWeekly(view, mode) {
        if (!Array.isArray(view) || view.length === 0) return [];
        if (mode === 'rolling') {
          // Group by week ending on Sunday
          const groups = {};
          view.forEach(row => {
            if (!row || !row.date) return;
            const d = new Date(row.date);
            // Find next Sunday (week end)
            const day = d.getDay();
            const offset = 7 - day - 1; // days until next Sunday
            const sunday = new Date(d);
            sunday.setDate(d.getDate() + offset);
            sunday.setHours(0,0,0,0);
            const key = sunday.toISOString().slice(0,10);
            if (!groups[key]) {
              groups[key] = [];
            }
            groups[key].push(row);
          });
          // Aggregate each group
          return Object.entries(groups).map(([weekEnd, rows]) => {
            // Aggregate wellness (average)
            const wellness = {};
            wellnessKeys.forEach(k => {
              wellness[k] = rows.map(r => (r && r.wellness && r.wellness[k] != null ? r.wellness[k] : 0)).reduce((a,b) => a+b,0) / rows.length;
            });
            // Aggregate productivity (sum)
            const productivity = {};
            productivityKeys.forEach(k => {
              productivity[k] = rows.map(r => (r && r.productivity && r.productivity[k] != null ? r.productivity[k] : 0)).reduce((a,b) => a+b,0);
            });
            return {
              date: weekEnd,
              wellness,
              productivity
            };
          }).sort((a,b) => a.date.localeCompare(b.date));
        } else {
          // Default: week starting Friday
          const groups = {};
          view.forEach(row => {
            if (!row || !row.date) return;
            const key = getWeekStartFriday(row.date);
            if (!groups[key]) {
              groups[key] = [];
            }
            groups[key].push(row);
          });
          // Aggregate each group
          return Object.entries(groups).map(([week, rows]) => {
            // Aggregate wellness (average)
            const wellness = {};
            wellnessKeys.forEach(k => {
              wellness[k] = rows.map(r => (r && r.wellness && r.wellness[k] != null ? r.wellness[k] : 0)).reduce((a,b) => a+b,0) / rows.length;
            });
            // Aggregate productivity (sum)
            const productivity = {};
            productivityKeys.forEach(k => {
              productivity[k] = rows.map(r => (r && r.productivity && r.productivity[k] != null ? r.productivity[k] : 0)).reduce((a,b) => a+b,0);
            });
            return {
              date: week,
              wellness,
              productivity
            };
          }).sort((a,b) => a.date.localeCompare(b.date));
        }
      }

      // Helper to build datasets with toggled state
      function buildDatasets() {
        const metricsView = aggregateWeekly(apiResponse.metrics_view, aggregationMode);
        const wellnessDatasets = wellnessKeys.map((key, idx) => {
          const base = {
            label: key,
            type: "line",
            data: metricsView.map(d => (d && d.wellness && d.wellness[key] != null ? d.wellness[key] : 0)),
            borderColor: fixedColors[key] || `hsl(${idx * 40}, 70%, 50%)`,
            borderWidth: 2,
            fill: false,
            yAxisID: "y",
            hidden: !toggledKeys.includes(key),
            pointRadius: 3,
            pointHoverRadius: 5
          };
          if (key === "readiness_score") {
            base.borderColor = "#87CEFA";
            base.pointRadius = 6;
            base.pointBorderColor = "#87CEFA";
            base.pointBackgroundColor = "#FFFFFF";
          }
          if (key === "sleep_score") {
            base.borderColor = "#A9A9A9";
            base.pointRadius = 6;
            base.pointBorderColor = "#A9A9A9";
            base.pointBackgroundColor = "#FFFFFF";
          }
          return base;
        });
        const productivityDatasets = productivityKeys.map((key, idx) => ({
          label: key,
          type: "bar",
          data: metricsView.map(d => (d && d.productivity && d.productivity[key] != null ? d.productivity[key] : 0)),
          backgroundColor: fixedColors[key] || `hsl(${idx * 60}, 60%, 60%)`,
          stack: "productivity",
          yAxisID: "y1",
          hidden: !toggledKeys.includes(key)
        }));
        return {
          labels: metricsView.map(d => d.date),
          datasets: [...wellnessDatasets, ...productivityDatasets]
        };
      }

      // Chart rendering
      const ctx = document.getElementById("dashboardChart");
      const chart = new Chart(ctx, {
        data: buildDatasets(),
        options: {
          responsive: true,
          interaction: { mode: "index", intersect: false },
          plugins: {
            legend: { display: false },
            tooltip: { mode: "index", intersect: false },
            datalabels: {
              display: function(context) {
                // Show label for every productivity bar segment
                return context.dataset.type === "bar" && context.dataset.stack === "productivity";
              },
              formatter: function(value, context) {
                // Show only the sum for each individual category inside its bar segment
                return value > 0 ? value.toFixed(2) : "";
              },
              anchor: "center",
              align: "center",
              color: "#fff",
              font: { weight: "bold", size: 14 }
            }
          },
          scales: {
            y: {
              type: "linear",
              position: "left",
              title: { display: true, text: "Wellness Scores" }
            },
            y1: {
              type: "linear",
              position: "right",
              stacked: true,
              grid: { drawOnChartArea: false },
              title: { display: true, text: "Productivity (hours)" }
            },
            x: { stacked: true }
          }
        },
        plugins: [
          ChartDataLabels,
          {
            id: 'barTotalLabel',
            afterDatasetsDraw: function(chart) {
              const ctx = chart.ctx;
              const metricsView = aggregateWeekly(apiResponse.metrics_view, aggregationMode);
              // Find all productivity bar metas
              const barMetas = chart.data.datasets
                .map((ds, idx) => ({ ds, meta: chart.getDatasetMeta(idx) }))
                .filter(obj => obj.ds.type === "bar" && obj.ds.stack === "productivity");
              if (!barMetas.length) return;
              chart.data.labels.forEach((label, i) => {
                // Sum all visible productivity bars for this group
                const visibleKeys = chart.data.datasets
                  .filter(ds => ds.type === "bar" && ds.stack === "productivity" && !ds.hidden)
                  .map(ds => ds.label);
                const d = metricsView[i];
                const total = visibleKeys
                  .map(k => d.productivity[k] || 0)
                  .reduce((a, b) => a + b, 0);
                // Find the top bar segment for this group
                let topY = null, x = null;
                for (const obj of barMetas) {
                  const bar = obj.meta.data[i];
                  if (bar && (!topY || bar.y < topY)) {
                    topY = bar.y;
                    x = bar.x;
                  }
                }
                if (total > 0 && topY !== null && x !== null) {
                  ctx.save();
                  ctx.font = 'bold 16px sans-serif';
                  ctx.textAlign = 'center';
                  ctx.textBaseline = 'bottom';
                  ctx.fillStyle = '#222';
                  ctx.fillText(total.toFixed(2), x, topY - 14);
                  ctx.restore();
                }
              });
            }
          }
        ]
      });

      // Render aggregation toggle above categories
      function renderAggregationToggle() {
        const container = document.getElementById("aggregationToggleContainer");
        container.innerHTML = "";
  const btnFriday = document.createElement("button");
  btnFriday.className = "aggregation-toggle-btn" + (aggregationMode === 'friday' ? " toggled" : "");
        btnFriday.textContent = "Week Starting Friday";
        btnFriday.onclick = function() {
          aggregationMode = 'friday';
          chart.data = buildDatasets();
          chart.update();
          renderAggregationToggle();
          renderCategories();
        };
  const btnRolling = document.createElement("button");
  btnRolling.className = "aggregation-toggle-btn" + (aggregationMode === 'rolling' ? " toggled" : "");
        btnRolling.textContent = "Rolling Week (ends today)";
        btnRolling.onclick = function() {
          aggregationMode = 'rolling';
          chart.data = buildDatasets();
          chart.update();
          renderAggregationToggle();
          renderCategories();
        };
        container.appendChild(btnFriday);
        container.appendChild(btnRolling);
      }

      // Render categories section below chart
      function renderCategories() {
        const section = document.getElementById("categoriesSection");
        section.innerHTML = "";
        // Wellness block
        const wellnessBlock = document.createElement("div");
        wellnessBlock.className = "category-block";
        wellnessBlock.innerHTML = `<div class='category-title'>Wellness</div>`;
        const wellnessList = document.createElement("ul");
        wellnessList.className = "category-list";
        wellnessKeys.forEach((key, idx) => {
          const li = document.createElement("li");
          let classNames = "category-item";
          if (toggledKeys.includes(key)) {
            classNames += " toggled";
            // Use border color from chart line for wellness buttons
            let borderColor = fixedColors[key] || `hsl(${idx * 40}, 70%, 50%)`;
            if (key === "readiness_score") borderColor = "#87CEFA";
            if (key === "sleep_score") borderColor = "#A9A9A9";
            li.style.background = borderColor;
            li.style.color = "#fff";
          }
          li.className = classNames;
          li.textContent = key.replace(/_/g, " ");
          li.style.cursor = "pointer";
          li.onclick = function() {
            if (toggledKeys.includes(key)) {
              toggledKeys = toggledKeys.filter(k => k !== key);
            } else {
              toggledKeys.push(key);
            }
            chart.data = buildDatasets();
            chart.update();
            renderCategories();
          };
          wellnessList.appendChild(li);
        });
        wellnessBlock.appendChild(wellnessList);
        section.appendChild(wellnessBlock);

        // Productivity block
        const prodBlock = document.createElement("div");
        prodBlock.className = "category-block";
        prodBlock.innerHTML = `<div class='category-title'>Productivity</div>`;
        const prodList = document.createElement("ul");
        prodList.className = "category-list";
        productivityKeys.forEach((key, idx) => {
          const li = document.createElement("li");
          let classNames = "category-item";
          if (toggledKeys.includes(key)) {
            classNames += " toggled";
            // Use bar fill color for productivity buttons
            let fillColor = fixedColors[key] || `hsl(${idx * 60}, 60%, 60%)`;
            li.style.background = fillColor;
            li.style.color = "#fff";
          }
          li.className = classNames;
          li.textContent = key.replace(/_/g, " ");
          li.style.cursor = "pointer";
          li.onclick = function() {
            if (toggledKeys.includes(key)) {
              toggledKeys = toggledKeys.filter(k => k !== key);
            } else {
              toggledKeys.push(key);
            }
            chart.data = buildDatasets();
            chart.update();
            renderCategories();
          };
          prodList.appendChild(li);
        });
        prodBlock.appendChild(prodList);
        section.appendChild(prodBlock);
      }

      renderAggregationToggle();
      renderCategories();
    });
});
