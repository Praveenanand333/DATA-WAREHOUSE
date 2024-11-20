import React, { useState, useEffect } from "react";
import Plot from "react-plotly.js";
import axios from "axios";

const App = () => {
  const [salesData, setSalesData] = useState([]);
  const [year, setYear] = useState(2023);
  const [month, setMonth] = useState(5);
  const [day, setDay] = useState(1);
  const [operation, setOperation] = useState("og");
  const [slicedimension, setSliceDimension] = useState("month");
  const [city, setCity] = useState("");
  const [state, setState] = useState("");
  const [country, setCountry] = useState("");
  const [dicedimension1, setDicedimension1] = useState("city");
  const [dicedimension2, setDicedimension2] = useState("year");
  const [rollupdimension, setRollupDimension] = useState("location");
  const [drilldowndimension, setDrilldownDimension] = useState("time");

  useEffect(() => {
    fetchSalesData();
  }, [year, month, operation, slicedimension, city, state, country, day, rollupdimension, drilldowndimension]);

  const fetchSalesData = async () => {
    const response = await axios.get("http://localhost:5000/fetch_sales_data", {
      params: { year, month, operation, slicedimension, city, state, country, day, dicedimension1, dicedimension2 },
    });
    setSalesData(response.data);
  };

  const prepare3DData = () => {
    let x = [], y = [], z = [], text = [];

    const xAxisLabel = rollupdimension === "location" ? "State" : drilldowndimension === "location" ? "City" : "Month";
    const yAxisLabel = rollupdimension === "time" ? "Month" : drilldowndimension === "time" ? "Day" : "State";

    salesData.forEach((record) => {
      x.push(rollupdimension === "location" ? record.State : drilldowndimension === "location" ? record.City : record.State);
      y.push(rollupdimension === "time" ? record.Month : drilldowndimension === "time" ? record.Day : record.Month);
      z.push(record.TotalPrice);
      text.push(`${record.City} - ${record.ItemName}`);
    });

    return { x, y, z, text, xAxisLabel, yAxisLabel };
  };

  const { x, y, z, text, xAxisLabel, yAxisLabel } = prepare3DData();

  return (
    <div className="p-6">
      <h1 className="text-3xl font-bold mb-6 text-center">Sales OLAP 3D Cube Visualization</h1>

      <div className="space-y-4 mb-8">
        <button
          onClick={() => setOperation("slice")}
          className="px-4 py-2 bg-blue-500 text-white rounded-md hover:bg-blue-600"
        >
          Slice
        </button>
        <div>
          <label className="mr-2">Select the dimension to slice: </label>
          <select
            value={slicedimension}
            onChange={(e) => setSliceDimension(e.target.value)}
            className="px-4 py-2 border rounded-md"
          >
            <option value="">Select Dimension</option>
            <option value="city">City</option>
            <option value="state">State</option>
            <option value="country">Country</option>
            <option value="year">Year</option>
            <option value="month">Month</option>
            <option value="day">Day</option>
          </select>
        </div>

        <button
          onClick={() => setOperation("dice")}
          className="px-4 py-2 bg-green-500 text-white rounded-md hover:bg-green-600"
        >
          Dice
        </button>
        <div className="space-x-4">
          <select
            value={dicedimension1}
            onChange={(e) => setDicedimension1(e.target.value)}
            className="px-4 py-2 border rounded-md"
          >
            <option value="">Select Dimension</option>
            <option value="city">City</option>
            <option value="state">State</option>
            <option value="country">Country</option>
          </select>
          <select
            value={dicedimension2}
            onChange={(e) => setDicedimension2(e.target.value)}
            className="px-4 py-2 border rounded-md"
          >
            <option value="">Select Dimension</option>
            <option value="year">Year</option>
            <option value="month">Month</option>
            <option value="day">Day</option>
          </select>
        </div>

        <div className="space-y-2 mt-4">
          <button
            onClick={() => { setRollupDimension("location"); setOperation("og"); }}
            className="px-4 py-2 mr-2 bg-yellow-500 text-white rounded-md hover:bg-yellow-600"
          >
            Roll-up-Location
          </button>
          <button
            onClick={() => { setRollupDimension("time"); setOperation("og"); }}
            className="px-4 mr-2 py-2 bg-purple-500 text-white rounded-md hover:bg-purple-600"
          >
            Roll-up-Time
          </button>
          <button
            onClick={() => { setDrilldownDimension("location"); setOperation("og"); }}
            className="px-4 mr-2 py-2 bg-red-500 text-white rounded-md hover:bg-red-600"
          >
            Drill-down-Location
          </button>
          <button
            onClick={() => { setDrilldownDimension("time"); setOperation("og"); }}
            className="px-4 py-2 mr-2 bg-purple-500 text-white rounded-md hover:bg-purple-600"
          >
            Drill-down-Time
          </button>
        </div>
      </div>

      <div className="space-y-2 mb-8">
        <label className="mr-2">Year: </label>
        <input
          type="number"
          value={year}
          onChange={(e) => setYear(e.target.value)}
          className="px-4 py-2 border rounded-md"
        />
        <label className="ml-4 mr-2">Month: </label>
        <input
          type="number"
          value={month}
          onChange={(e) => setMonth(e.target.value)}
          className="px-4 py-2 border rounded-md"
        />
        <label className="ml-4 mr-2">City: </label>
        <input
          type="text"
          value={city}
          onChange={(e) => setCity(e.target.value)}
          className="px-4 py-2 border rounded-md"
        />
        <label className="ml-4 mr-2">Day: </label>
        <input
          type="text"
          value={day}
          onChange={(e) => setDay(e.target.value)}
          className="px-4 py-2 border rounded-md"
        />
        <label className="ml-4 mr-2">State: </label>
        <input
          type="text"
          value={state}
          onChange={(e) => setState(e.target.value)}
          className="px-4 py-2 border rounded-md"
        />
        <br/>
        <label className="ml-4 mr-2">Country: </label>
        <input
          type="text"
          value={country}
          onChange={(e) => setCountry(e.target.value)}
          className="px-4 py-2 border rounded-md"
        />
      </div>

      <Plot
        data={[
          {
            type: "scatter3d",
            mode: "markers",
            x: x,
            y: y,
            z: z,
            text: text,
            marker: {
              size: 5,
              color: z, // Map colors directly to the 'z' dimension
              colorscale: "Rainbow", // Apply a vibrant colorscale
               // Add a color legend
            },
          },
        ]}
        layout={{
          autosize:false,
          width:1200,
          height:800,
          
          scene: {
            xaxis: { title: xAxisLabel },
            yaxis: { title: yAxisLabel },
            zaxis: { title: "Total Sales" },
          },
        }}
      />
    </div>
  );
};

export default App;
