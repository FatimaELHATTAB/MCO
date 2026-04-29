<mxfile host="app.diagrams.net">
  <diagram name="TVA State Machine">
    <mxGraphModel dx="1200" dy="800" grid="1" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1">
      <root>
        <mxCell id="0"/>
        <mxCell id="1" parent="0"/>

        <!-- ENTRY -->
        <mxCell id="tier" value="TIER RECEIVED" style="rounded=1;fillColor=#dae8fc;" vertex="1" parent="1">
          <mxGeometry x="20" y="200" width="140" height="60" as="geometry"/>
        </mxCell>

        <!-- MATCHING -->
        <mxCell id="rpmp" value="PROVIDED_BY_RPMP" style="rounded=1;fillColor=#d5e8d4;" vertex="1" parent="1">
          <mxGeometry x="200" y="80" width="180" height="60" as="geometry"/>
        </mxCell>

        <mxCell id="orbis" value="MATCHING_ORBIS" style="rounded=1;fillColor=#d5e8d4;" vertex="1" parent="1">
          <mxGeometry x="200" y="200" width="180" height="60" as="geometry"/>
        </mxCell>

        <mxCell id="refinitiv" value="MATCHING_REFINITIV" style="rounded=1;fillColor=#d5e8d4;" vertex="1" parent="1">
          <mxGeometry x="200" y="320" width="180" height="60" as="geometry"/>
        </mxCell>

        <!-- BATCH -->
        <mxCell id="calc" value="CALCUL TVA" style="rounded=1;fillColor=#fff2cc;" vertex="1" parent="1">
          <mxGeometry x="420" y="200" width="140" height="60" as="geometry"/>
        </mxCell>

        <mxCell id="vies" value="CALL VIES" style="rounded=1;fillColor=#fff2cc;" vertex="1" parent="1">
          <mxGeometry x="600" y="200" width="140" height="60" as="geometry"/>
        </mxCell>

        <!-- RESULTS -->
        <mxCell id="viesok" value="CONFIRMED_BY_VIES" style="rounded=1;fillColor=#f8cecc;" vertex="1" parent="1">
          <mxGeometry x="800" y="80" width="200" height="60" as="geometry"/>
        </mxCell>

        <mxCell id="orbisok" value="CONFIRMED_BY_ORBIS" style="rounded=1;fillColor=#f8cecc;" vertex="1" parent="1">
          <mxGeometry x="800" y="200" width="200" height="60" as="geometry"/>
        </mxCell>

        <mxCell id="refok" value="CONFIRMED_BY_REFINITIV" style="rounded=1;fillColor=#f8cecc;" vertex="1" parent="1">
          <mxGeometry x="800" y="320" width="200" height="60" as="geometry"/>
        </mxCell>

        <mxCell id="notfound" value="NOT_FOUND" style="rounded=1;fillColor=#f8cecc;" vertex="1" parent="1">
          <mxGeometry x="800" y="440" width="200" height="60" as="geometry"/>
        </mxCell>

        <mxCell id="noteligible" value="NOT_ELIGIBLE" style="rounded=1;fillColor=#f8cecc;" vertex="1" parent="1">
          <mxGeometry x="800" y="560" width="200" height="60" as="geometry"/>
        </mxCell>

        <!-- EDGES -->
        <mxCell id="e1" edge="1" parent="1" source="tier" target="rpmp"/>
        <mxCell id="e2" edge="1" parent="1" source="tier" target="orbis"/>
        <mxCell id="e3" edge="1" parent="1" source="tier" target="refinitiv"/>

        <mxCell id="e4" edge="1" parent="1" source="rpmp" target="calc"/>
        <mxCell id="e5" edge="1" parent="1" source="orbis" target="calc"/>
        <mxCell id="e6" edge="1" parent="1" source="refinitiv" target="calc"/>

        <mxCell id="e7" edge="1" parent="1" source="calc" target="vies"/>

        <mxCell id="e8" edge="1" parent="1" source="vies" target="viesok"/>
        <mxCell id="e9" edge="1" parent="1" source="vies" target="orbisok"/>
        <mxCell id="e10" edge="1" parent="1" source="vies" target="refok"/>
        <mxCell id="e11" edge="1" parent="1" source="vies" target="notfound"/>
        <mxCell id="e12" edge="1" parent="1" source="vies" target="noteligible"/>

        <!-- BACK FLOWS -->
        <mxCell id="b1" edge="1" parent="1" source="orbisok" target="refinitiv"/>
        <mxCell id="b2" edge="1" parent="1" source="refok" target="orbis"/>
        <mxCell id="b3" edge="1" parent="1" source="viesok" target="notfound"/>
        <mxCell id="b4" edge="1" parent="1" source="notfound" target="orbis"/>
        <mxCell id="b5" edge="1" parent="1" source="notfound" target="refinitiv"/>
        <mxCell id="b6" edge="1" parent="1" source="orbisok" target="rpmp"/>
        <mxCell id="b7" edge="1" parent="1" source="refok" target="rpmp"/>

      </root>
    </mxGraphModel>
  </diagram>
</mxfile>
