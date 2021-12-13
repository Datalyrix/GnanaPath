(function(){
  document.addEventListener('DOMContentLoaded', function(){
    let $$ = selector => Array.from( document.querySelectorAll( selector ) );
    let $ = selector => document.querySelector( selector );

    let tryPromise = fn => Promise.resolve().then( fn );

    let toJson = obj => obj.json();
    let toText = obj => obj.text();

    let cy;
    let hdrlbl = document.getElementById('gnhdr_lbl');
    let selstatus_lbl = document.getElementById('selstatuslbl');
    let gnnode_selid = document.getElementById("gnnode_selid");
    let cy_nodes;
    let cy_edges;
    let cy_dnodes = 0;
    let cy_dedges = 0;
    let cy_mnodes = 0;
    let cy_medges = 0;
    var gn_nodes_tbl = [];
    var gn_edges_tbl = [];  
    var ncolors = {"GNMetaNode": "#923222"};
    ///"#aa0755";

    let $stylesheet = "style.json";
    let getStylesheet = name => {
      let convert = res => name.match(/[.]json$/) ? toJson(res) : toText(res);

      return fetch(`/static/cola/stylesheets/${name}`).then( convert );
    };
      
    let applyStylesheet = stylesheet => {
      if( typeof stylesheet === typeof '' ){
        cy.style().fromString( stylesheet ).update();
      } else {
          cy.style().fromJson( stylesheet ).update();
	  cy.style()
	      .selector('node')
	      .style('background-color', function(ele) { if(ele.data('ntype') == "GNMetaNode") return "#0070C0"; else return "#aa0755"; })
	      .update();
	  cy.minZoom(0.00);
	  cy.maxZoom(10);
      }
    };
      
    let applyStylesheetFromSelect = () => Promise.resolve( $stylesheet ).then( getStylesheet ).then( applyStylesheet );


      
    let $dataset = "";
    let getDataset = name => fetch(`datasets/${name}`).then( toJson );
    let getGnDataset = name => gnFetchData();
      
    let applyDataset = dataset => {
      // so new eles are offscreen
      cy.zoom(1);
      cy.pan({ x: -9999999, y: -9999999 });
      console.log('View: new Dataset is applied');
      // replace eles
      cy.elements().remove();
      cy.add( dataset );
      //selstatus_lbl.innerHTML = 'Data Upload Complete.';
      ///hdrlbl.innerHTML = 'Total Nodes:'+cy_nodes+' Total Edges:'+cy_edges+'';	
    }
      
    let applyDatasetFromSelect = () => 
        Promise.resolve( $dataset ).then( getGnDataset ).then( applyDataset ).then(setNodesMethod);

      let applyMetaDataset = () =>
	  Promise.resolve( $dataset ).then(gnFetchMetaNodes ).then( applyDataset).then(setNodesMethod);
	  ///.then(applyStylesheetFromSelect).then(applyLayoutFromSelect);
    ///let applyDatasetFromSelect = () => 
	///Promise.resolve( $dataset).then( getGnDataSet().then(
	    
    let $layout = "cola";
    let maxLayoutDuration = 1500;
    let layoutPadding = 50;
    let layouts = {
      cola: {
        name: 'cola',
        padding: layoutPadding,
        nodeSpacing: 12,
        edgeLengthVal: 25,
        animate: true,
        randomize: true,
        maxSimulationTime: maxLayoutDuration,
        boundingBox: { // to give cola more space to resolve initial overlaps
          x1: 0,
          y1: 0,
          x2: 8000,
          y2: 2000
        },
	  
        edgeLength: function( e ){
          let w = e.data('weight');

          if( w == null ){
            w = 0.5;
          }

          return 25 / w;
        }
      }
    };
      
    console.log('View: Init called');
    let cy_metanodes;
       
    cy_nodes = 0;
    cy_edges = 0;
      
    //gnFetchMetaNodes();
      
    let prevLayout;
    let getLayout = name => Promise.resolve( layouts[ name ] );
    let applyLayout = layout => {
      if( prevLayout ){
        prevLayout.stop();
      }
      console.log('View: Apply New Layout');
      let l = prevLayout = cy.makeLayout( layout );
      return l.run().promiseOn('layoutstop');
    }
      
    let applyLayoutFromSelect = () => Promise.resolve( $layout ).then( getLayout ).then( applyLayout );

    let $algorithm = $('#algorithm');
    let getAlgorithm = (name) => {
      switch (name) {
        case 'bfs': return Promise.resolve(cy.elements().bfs.bind(cy.elements()));
        case 'dfs': return Promise.resolve(cy.elements().dfs.bind(cy.elements()));
        case 'none': return Promise.resolve(undefined);
        default: return Promise.resolve(undefined);
      }
    };
    let runAlgorithm = (algorithm) => {
      if (algorithm === undefined) {
        return Promise.resolve(undefined);
      } else {
        let options = {
          root: '#' + cy.nodes()[0].id(),
          // astar requires target; goal property is ignored for other algorithms
          goal: '#' + cy.nodes()[Math.round(Math.random() * (cy.nodes().size() - 1))].id()
        };
        return Promise.resolve(algorithm(options));
      }
    }
    let currentAlgorithm;
    let animateAlgorithm = (algResults) => {
      // clear old algorithm results
      cy.$().removeClass('highlighted start end');
      currentAlgorithm = algResults;
      if (algResults === undefined || algResults.path === undefined) {
        return Promise.resolve();
      }
      else {
        let i = 0;
        // for astar, highlight first and final before showing path
        if (algResults.distance) {
          // Among DFS, BFS, A*, only A* will have the distance property defined
          algResults.path.first().addClass('highlighted start');
          algResults.path.last().addClass('highlighted end');
          // i is not advanced to 1, so start node is effectively highlighted twice.
          // this is intentional; creates a short pause between highlighting ends and highlighting the path
        }
        return new Promise(resolve => {
          let highlightNext = () => {
            if (currentAlgorithm === algResults && i < algResults.path.length) {
              algResults.path[i].addClass('highlighted');
              i++;
              setTimeout(highlightNext, 50);
            } else {
              // resolve when finished or when a new algorithm has started visualization
              resolve();
            }
          }
          highlightNext();
        });
      }
    };
    let applyAlgorithmFromSelect = () => Promise.resolve( $algorithm.value ).then( getAlgorithm ).then( runAlgorithm ).then( animateAlgorithm );

    cy = window.cy = cytoscape({
      container: $('#cy')
    });

    //// Add panzoom   
    cy.panzoom({
       //// zoom options	  
      });

      ///tryPromise( applyDatasetFromSelect ).then( applyStylesheetFromSelect ).then( applyLayoutFromSelect );

      tryPromise( applyMetaDataset).then( applyStylesheetFromSelect ).then( applyLayoutFromSelect );
      
      let applyDatasetChange = () => tryPromise(applyDatasetFromSelect).then(applyStylesheetFromSelect).then(applyLayoutFromSelect);
      
    ///$('#redo-layout').addEventListener('click', applyLayoutFromSelect);
////Disable algorithm
    ///$algorithm.addEventListener('change', applyAlgorithmFromSelect);
    ///$('#redo-algorithm').addEventListener('click', applyAlgorithmFromSelect);
      $('#clear-graph').addEventListener('click', removeAllNodes);

      
    //////////////////////////////////
    ///$('#srchbtn').addEventListener('click', applyDatasetChange);
     $('#srchbtn').addEventListener('click', gnFetchDataforMetaNodes);

      function  removeAllNodes() {

	  cy.elements().remove();
	  cy_dnodes = 0;
	  cy_dedges = 0;
	  applyMetaDataset();
	  applyStylesheetFromSelect();
	  applyLayoutFromSelect();
	  
      }
    
      function gnapplynode() {

          var d = '';
	  d += '{'+"\n";
	  d += '"data": {'+"\n";
	  d += '"id": "n121", ';
          d += '"idInt": 121, ';
	  d += '"name": "TestNode" ';
	  d += '}, '+"\n";
	  d += '"group" : "nodes",'+"\n";
	  d += '"removed": false,'+"\n";
	  d += '"selected": false,'+"\n";
	  d += '"selectable": true,'+"\n";
	  d += '"grabbable": true,'+"\n";
	  d += '"locked": false'+"\n";
	  d += '}'+"\n";
	  s = '['+"\n";
	  s += d;
	  s += ']'+"\n";
	  console.log(' Add new s: '+s);
	  
	  gn_e = JSON.parse(s);
	  console.log('Add new node ');
	  cy.add(gn_e);
	  applyLayoutFromSelect();
	  
      }
      
     ///// Update view with datanodes based on search filter
      
    function  gnFetchDataforMetaNodes() {

	console.log('Gndata fetch for metanodes');
	var stlbl = document.getElementById('gnhdr_lbl');
	var errlbl = document.getElementById('gnerr_lbl');
	var loader_id = document.getElementById('loaderid');
	var srch_inp = document.getElementById('gnsrch_inp_id');
	var srchstr = srch_inp.value;
	var lnodesid = document.getElementById('gnnode_lnodesid');
	var lnodes = lnodesid.value;
	var nodemode_id = document.getElementById('gnnode_modeid');
	
        var nmode = nodemode_id.value; /// Nodemode 1 for nodesonly 2 for Nodes+edges+derived nodes
	
	
	var gnHeaders = new Headers({
            'Content-Type': 'text/plain',
            'Access-Control-Allow-Origin' : '*'
        });
	
        
	if (srchstr == '') {
	    console.log('GnFetchDataforMetaNodes: empty search string ');
	    s = {};
	    return(s);
	}
	
	console.log('GNFetchData View: sql txt srch '+srchstr);
	console.log('GNFetchData View:  nmode '+nmode+'   limit nodes '+lnodes);

	
	var nodes_url = "/api/v1/metanodes?srchqry='"+srchstr+"'";
	var dataedges_url = "/api/v1/search?srchqry='"+srchstr+"'&lnodes="+lnodes+"&nodemode="+nmode;
	var nodes;
	var edges;
	var sele = '';
	
	var myHeaders = new Headers({
	    'Content-Type': 'text/plain',
	    'Access-Control-Allow-Origin' : '*'
	});

	stlbl.innerHTML ="Loading data for metanodes";
	loader_id.style.display = "block";
	cy_nodes = 0;
	cy_edges = 0;

	try {
   	   return fetch(dataedges_url, { method: 'GET', headers: myHeaders })
	
            .then (response => response.json())
            .then (data => {
                //console.log('GNView: Fetch complete ');
		//console.log('GNView: data:'+JSON.stringify(data, null,3));
		var status = data.status;
		var statusmsg = data.statusmsg;
		var gndata = data.gndata;
		var nodelen = gndata.nodelen;
		var edgeslen = gndata.edgelen;

		if (status == "ERROR") {
                    console.log('GNView: Error status ');
                    errlbl.innerHTML = "Error getting data from database";
		    loader_id.style.display = "none";
		    s = '[]';
		    gn_elements = JSON.parse(s);
		    return(gn_elements);
		}
		
		nodes = '';	      
		///s = '{'+"\n";
		//nodes += ' '+"\n";
		console.log('GNView: datanodes fetched len:'+nodelen);
		medges = '';
		let x = 0;
		for (i=0; i < nodelen; i++) {
		    
                    if ( i > 0)
			nodes += ","+"\n";
		    
                    nodes += '{';
		    n = JSON.parse(gndata.nodes[i]);
                    nodes += '"data": {';
                    var idint = parseInt(n.id);
                    //console.log('GNView node-'+i+'  '+JSON.stringify(n));
		    //console.log('GNView node-'+i+'  id '+n.id);
                    nodes+= '"id": "n'+n.id+'", ';
		    nodes+= '"idInt": '+(idint)+',';
		    nodes+= '"name": "'+n.nodename+'", ';
		    nodes+= '"query": true ';
                    
		    ///for (const [key, value] of Object.entries(n)) {
		   ///	console.log(`${key} ${value}`); // "a 5", "b 7", "c 9"
		    ///}
                    gn_nodes_tbl[n.id] = n;
		    
                    nodes+= '},'+"\n";
		    nodes+= '"group" : "nodes",';
		    nodes+= '"removed": false,';
		    nodes+= '"selected": false,';
		    nodes+= '"selectable": true,';
		    nodes+= '"grabbable": true,';
		    nodes+= '"locked": false';
		    nodes+= '}';
		    cy_dnodes += 1;
		    if (n.nodetype == "GNDataNode") {
			
			//// A Datanode needs an edge from MetaNode if it exists on the map
			let metanodeid = "n"+n.gnmetanodeid;
			let mele = cy.getElementById(metanodeid);
			if (mele != null) {
			    			    
			    console.log(' metanode id is present ');
			}

		    }
		}
		
		///nodes += ']'+"\n";
		////////// Get edges 
		//console.log('GNView: Fetch Edges complete ');
		//console.log('GNView: data:'+JSON.stringify(data, null,3));
     
		
		var edgelen = 0;
		edges = '';
		
		if (gndata.edges)
		    edgelen = gndata.edgelen;
		else
		    edgelen = 0;
		
		///s = '{'+"\n";
		///edges += '['+"\n";
		//console.log('GNView: fetch Edges complete len:'+nodelen);

		for (i=0; i < edgelen; i++) {	  
		    if ( i > 0)
			edges += ","+"\n";
		    
		    edges += '{';
		    e = JSON.parse(gndata.edges[i]);
	            edges += '"data": {';
		    var idint = parseInt(e.id);
		    ///console.log('GNView node-'+i+' id '+n.id);
		    edges += '"id": "e'+e.id+'", ';
		    edges += '"idInt": '+(idint)+',';
		    edges += '"relname": "'+(e.type)+'" ,';
		    edges += '"source": "n'+e.source+'" ,';
		    edges += '"target": "n'+e.target+'" ,';
		    edges += '"directed": true ';
		    //edges += '"name": "'+n.name+'", ';
		    //edges += '"query": true ';
		    edges += '},'+"\n";
		    
                    edges += '"position": {},';
		    edges += '"group": "edges",';
		    edges += '"removed": false,';
		    edges += '"selected": false,';
		    edges += '"selectable": true, ';
		    edges += '"locked": false, ';
		    edges += '"grabbable": true, ';
		    edges += '"directed": true';
		    edges += '}';
		    cy_dedges += 1;
		}
		
		///edges += ']'+"\n";
		s = '['+"\n";
		///////s += '{'+"\n";
		s += nodes;
		if (edges) 
		    s += ','+"\n";
		s += edges;		
		s += '\n';
		s += ']'+"\n";
		console.log('GNView nodes & edges:  '+s);
		gn_elements = JSON.parse(s);
		//gn_elements  = s;
                cy.add(gn_elements);
		applyLayoutFromSelect();
		setNodesMethod();
		
		console.log('GnView: fetch process is completed');
		loader_id.style.display = "none";
		stlbl.innerHTML = "MetaNodes: "+cy_mnodes+"&emsp;&emsp; DataNodes:"+cy_dnodes+" &emsp;&emsp;&emsp;&emsp; Data Upload: SUCCESS";
		return (gn_elements);
		////////////////////////////////
		
	    });
	} catch(error) {
	    stlbl.innerHTML = 'Network error fetching data';
	    console.log('Error caught '+error);
	}
	
	
    }

    function  gnGraphRemoveAll() {  
	console.log(' Removing all elements and clear the graph ');
    }
	
    function  gnFetchData() {

        //var srchstr = "SELECT * from customer;";
	///var url = '/static/cola/js/product.json';
	var srch_inp = document.getElementById('gnsrch_inp_id');
	var srchstr = srch_inp.value;
	var gnnode_sel = document.getElementById('gnnode_selid').value;
	var gnnode_sel = gnnode_selid.value;
	var stlbl = document.getElementById('selstatuslbl');
        var errlbl = document.getElementById('selerrorlbl');
	///var hdrlbl = document.getElementById('gnhdr_lbl');

	console.log('GNFetchData called at init');
        if (srchstr == '') {
	    console.log('GNFetchData has empty search string ');
	    s = {};
	    return (s);
	}
	
	
        if (gnnode_sel == "select") {
	    srchstr = "";
	    s = {};
	    return(s) ;
	}
        else
	    srchstr = "SELECT * from "+gnnode_sel+" LIMIT 1000";
	
	console.log('GNFetchData View: sql txt srch '+srchstr);
	var nodes_url = "/api/v1/metanodes?srchqry='"+srchstr+"'";
	var dataedges_url = "/api/v1/search?srchqry='"+srchstr+"'";
	var nodes;
	var edges;
	var sele = '';
	var myHeaders = new Headers({
	    'Content-Type': 'text/plain',
	    'Access-Control-Allow-Origin' : '*'
	});

	hdrlbl.innerHTML ="Loading data from network";
	cy_nodes = 0;
	cy_edges = 0;

	try {
   	   return fetch(dataedges_url, { method: 'GET', headers: myHeaders })
	
            .then (response => response.json())
            .then (data => {
                //console.log('GNView: Fetch complete ');
		//console.log('GNView: data:'+JSON.stringify(data, null,3));
		var status = data.status;
		var statusmsg = data.statusmsg;
		var gndata = data.gndata;
		var nodelen = gndata.nodes.length;
		var edgeslen = gndata.edges.length;

		if (status == "ERROR") {
                    console.log('GNView: Error status ');
                    errlbl.innerHTML = "Error getting data from database";
		    s = '[]';
		    gn_elements = JSON.parse(s);
		    return(gn_elements);
		}
		
		nodes = '';	      
		///s = '{'+"\n";
		//nodes += ' '+"\n";
		console.log('GNView: fetch complete len:'+nodelen);
		cy_nodes = nodelen;
		cy_edges = edgeslen;
		
		for (i=0; i < nodelen; i++) {
		    
                    if ( i > 0)
			nodes += ","+"\n";
		    
                    nodes += '{';
		    n = JSON.parse(gndata.nodes[i]);
                    nodes += '"data": {';
                    var idint = parseInt(n.id);
                    //console.log('GNView node-'+i+'  '+JSON.stringify(n));
		    //console.log('GNView node-'+i+'  id '+n.id);
                    nodes+= '"id": "n'+n.id+'", ';
		    nodes+= '"idInt": '+(idint)+',';
		    nodes+= '"name": "'+n.nodename+'", ';
		    nodes+= '"query": true ';
                    nodes+= '},'+"\n";
		    nodes+= '"group" : "nodes",';
		    nodes+= '"removed": false,';
		    nodes+= '"selected": false,';
		    nodes+= '"selectable": true,';
		    nodes+= '"grabbable": true,';
		    nodes+= '"locked": false';
		    nodes+= '}';
		}
		
		///nodes += ']'+"\n";
		////////// Get edges 
		//console.log('GNView: Fetch Edges complete ');
		//console.log('GNView: data:'+JSON.stringify(data, null,3));
     
		
		var edgelen = 0;
		edges = '';
		
		if (gndata.edges)
		    edgelen = gndata.edges.length;
		else
		    edgelen = 0;
		
		///s = '{'+"\n";
		///edges += '['+"\n";
		//console.log('GNView: fetch Edges complete len:'+nodelen);
		cy_edges = edgelen;
		for (i=0; i < edgelen; i++) {	  
		    if ( i > 0)
			edges += ","+"\n";
		    
		    edges += '{';
		    e = JSON.parse(gndata.edges[i]);
	            edges += '"data": {';
		    var idint = parseInt(e.id);
		    ///console.log('GNView node-'+i+' id '+n.id);
		    edges += '"id": "e'+e.id+'", ';
		    edges += '"idInt": '+(idint)+',';
		    edges += '"relname": "'+(e.type)+'" ,';
		    edges += '"source": "n'+e.source+'" ,';
		    edges += '"target": "n'+e.target+'" ,';
		    edges += '"directed": true ';
		    //edges += '"name": "'+n.name+'", ';
		    //edges += '"query": true ';
		    edges += '},'+"\n";
		    
                    edges += '"position": {},';
		    edges += '"group": "edges",';
		    edges += '"removed": false,';
		    edges += '"selected": false,';
		    edges += '"selectable": true, ';
		    edges += '"locked": false, ';
		    edges += '"grabbable": true, ';
		    edges += '"directed": true';
		    edges += '}';		   
		}
		
		///edges += ']'+"\n";
		s = '['+"\n";
		///////s += '{'+"\n";
		s += nodes;
		if (edges) 
		    s += ','+"\n";
		s += edges;		
		s += '\n';
		s += ']'+"\n";

		console.log('GNView nodes & edges:  '+s);
		gn_elements = JSON.parse(s);
		//gn_elements  = s;
		console.log('GnanaMetaView: fetch process is completed');
		hdrlbl.innerHTML = "Network data fetched. Loading Visualization..";
		return (gn_elements);
		////////////////////////////////
		
	    });
	} catch(error) {
	    hdrlbl.innerHTML = 'Network error fetching data';
	    console.log('Error caught '+error);
	}
	
    }

    function  gnFetchMetaNodes() {
        var nodes_url = "/api/v1/metanodes";
        var metanodes;
	
        var stlbl = document.getElementById('gnhdr_lbl');
        var errlbl = document.getElementById('gnerr_lbl');
	var loader_id = document.getElementById('loaderid');
	
	var gnHeaders = new Headers({
            'Content-Type': 'text/plain',
            'Access-Control-Allow-Origin' : '*'
        });

	stlbl.innerHTML = " Fetching  metanodes for search ";
        loader_id.style.display = "block";
	
	try {
         
	     return fetch(nodes_url, { method: 'GET', headers: gnHeaders })
               .then (response => response.json())
                .then (data => {
                     console.log('GNView: Fetch gnmeta nodes complete ');

                    var status = data.status;
                    var nodelen;
		    let gndata;
		    if (status == "ERROR") {
			 console.log('GNView: Error status ');
			 stlbl.innerHTML = "";
                        errlbl.innerHTML = "Error getting metanode data from server";
			loader_id.style.display = "none";
                        return;
		    }
		    
		    ///gnnode_selid.empty();
		    ///$("#gnnode_selid").empty();
		    ///$("#gnnode_selid").html("");
		    gndata = data.gndata;
		    nodelen = gndata.nodeslen;

		    cy_metanodes = '';
	            cy_mnodes = nodelen;	    
		    for (i=0; i < nodelen; i++) {
			
                        n = JSON.parse(gndata.nodes[i]);
		
			console.log('srchview: showing metanode '+JSON.stringify(n, null, 2));
			
			if (n.nodetype == "GNMetaNode") {
			    if (i > 0)
				cy_metanodes += ","+"\n";

			    cy_metanodes += '{';
			    cy_metanodes += '"data": {';
			    var idint = parseInt(n.id);
			    cy_metanodes+= '"id": "n'+n.id+'", ';
			    cy_metanodes+= '"idInt": '+(idint)+',';
			    cy_metanodes+= '"name": "'+n.nodename+'", ';
			    cy_metanodes+= '"ntype": "'+n.nodetype+'", ';
			    //cy_metanodes+= '"nodetype": "'+n.nodetype'", ';
			    cy_metanodes+= '"query": true ';
			    cy_metanodes+= '},'+"\n";
			    cy_metanodes+= '"group" : "nodes",';
			    cy_metanodes+= '"removed": false,';
			    cy_metanodes+= '"selected": false,';
			    cy_metanodes+= '"selectable": true,';
			    cy_metanodes+= '"grabbable": true,';
			    cy_metanodes+= '"locked": false';
			    cy_metanodes+= '}';
			    
                            /// Add to select options
			    console.log('srchview: adding node '+n.nodename);
                            ///gnnode_selid.append($("<option></option>").attr("value", n.name).text(n.value));
			    //$('#gnnode_selid').append("<option>" + n.name + "</option>");
			    var c = document.createElement("option");
			    c.text = n.nodename;
			    ////gnnode_selid.options.add(c,1);
			    
			}
		    }
		    s = '['+"\n";
		    s += cy_metanodes;
		    s += "\n";
		    s += ']'+"\n";
		    gn_elements = JSON.parse(s);
		    //console.log(' GnFetch MetaNodes '+JSON.stringify(gn_elements, null, 2));
		    loader_id.style.display = "none";
		    stlbl.innerHTML = " MetaNodes: "+cy_mnodes+" &emsp;&emsp; Click on nodes to get data";
		    return(gn_elements);
		});

	} catch(error) {
            hdrlbl.innerHTML = 'Network error fetching data';
            console.log('Error caught '+error);
        }


    }
      
    /////// Node selection methods
    function setNodesMethod() {  
	cy.nodes().forEach(function(n) {
            var g = n.data('nodename');
	    
            n.on('click', function(e) {
		var id = e.cyTarget.id();
		var ndata = e.cyTarget.data();
		var selnode_name = document.getElementById('selnodename');
		var selnode_id = document.getElementById('selnodeid');
		var selnode_type = document.getElementById('selnodetype');
		var gnsrch_inp = document.getElementById('gnsrch_inp_id');
		selnode_name.innerHTML = ndata.name;
		selnode_id.innerHTML = ndata.id;
		selnode_type.innerHTML = ndata.ntype;
		gnsrch_inp.value="SELECT * from "+ndata.name;
            });
	});
    }
      



      ///////////////Zooming
      var zx, zy;
/////// Disable nodespace for timebeing
 //// $('#gnnodespaceid').addEventListener('change', applyNodeSpaceChange);
 /// $('#gnzoomid').addEventListener('change', applyZoomLevelChange);

  function calculateZoomCenterPoint(){
            var pan = cy.pan();
            var zoom = cy.zoom();

      zx = cy.width()/2;
      zy = cy.height()/2;
  }

      
  function   applyZoomLevelChange() {

      var cur_zoomval = document.getElementById('gnzoomid').value;
      var pan = cy.pan();
      var zoom = cy.zoom();
      var zmin = cy.minZoom();
      var zmax = cy.maxZoom();
      var zmax_f = parseFloat(zmax).toFixed(2);
      var zmin_f = parseFloat(zmin).toFixed(2);
	  
      var new_zoomlvl = (((zmax_f - zmin_f)*cur_zoomval)/100);
      
      console.log('Zoom level is  '+cur_zoomval);
      console.log('Zoom cur val:'+zoom+'   pan val:'+JSON.stringify(pan, null, 2));
      console.log('Zoom minval:'+zmin.toFixed(5)+' maxval:'+zmax_f);
      console.log('Zoom new val:'+new_zoomlvl+' ');
      //cy.zoomingEnabled(true);
      
      calculateZoomCenterPoint();
      
      cy.zoom({
	  level: new_zoomlvl,
	  renderedPosition: {x: zx, y:zy}
      });
      ///cy.viewport({
	 /// zoom: new_zoomlvl,
	  ///pan: pan
      ///});
      //if (prevLayout) {
//	  prevLayout.stop();
  //    }

      
      ///return applyLayoutFromSelect();  
  };


      
  function   applyNodeSpaceChange() {

      var nodespaceval = document.getElementById('gnnodespaceid').value;
      var pan = cy.pan();
      var zoom = cy.zoom();
      var zmin = cy.minZoom();
      var zmax = cy.maxZoom();

      console.log('Nodespace value change '+nodespaceval);
      console.log('Zoom val:'+zoom+'   pan val:'+JSON.stringify(pan, null, 2));
      console.log('Zoom minval:'+zmin+' maxval:'+zmax);
  };


      
 });

    
})();

// tooltips with jQuery
$(document).ready(() => $('.tooltip').tooltipster());
    
