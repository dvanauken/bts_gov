(function() {
  async function loadFlights() {
      const contentDisplay = document.getElementById('flights-data');
      const progressOverlay = document.getElementById('progress-overlay');
      const progressBar = document.getElementById('progress-bar');
      const progressText = document.getElementById('progress-text');

      try {
          console.log('Starting to load flights...');
          progressOverlay.style.display = 'flex';
          progressBar.style.width = '0%';

          // Fetch data
          console.log('Fetching flights...');
          const response = await fetch('/api/flights/stream');
          const data = await response.json();
          
          progressBar.style.width = '90%';
          progressText.textContent = 'Rendering data...';

          // Convert grouped data into HTML
          let html = '';
          for (const [itinId, flights] of Object.entries(data)) {
              // Add itinerary header
              html += `
                  <tr class="itinerary-header">
                      <td colspan="12" style="background-color: #f0f0f0; font-weight: bold;">
                          Itinerary ID: ${itinId}
                      </td>
                  </tr>
              `;
              
              // Add flights for this itinerary
              flights.forEach(flight => {
                  html += `
                      <tr>
                          <td>${flight.year}</td>
                          <td>${flight.quarter}</td>
                          <td>${flight.ItinID}</td>
                          <td>${flight.SeqNum}</td>
                          <td>${flight.Coupons}</td>
                          <td>${flight.Origin}</td>
                          <td>${flight.Dest}</td>
                          <td>${flight.CouponType}</td>
                          <td>${flight.TkCarrier}</td>
                          <td>${flight.OpCarrier}</td>
                          <td>${flight.RPCarrier}</td>
                          <td>${flight.Passengers}</td>
                      </tr>
                  `;
              });
          }

          contentDisplay.innerHTML = html;

          progressBar.style.width = '100%';
          setTimeout(() => {
              progressOverlay.style.display = 'none';
          }, 500);

      } catch (error) {
          console.error('Error loading flights:', error);
          progressText.textContent = `Error: ${error.message}`;
      }
  }

  // Start loading when page loads
  loadFlights();
})();