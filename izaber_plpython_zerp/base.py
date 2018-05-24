import pprint

import izaber.plpython.base

class IPLPY(izaber.plpython.base.IPLPY):
    def rounding(self, f, r):
        if not r:
            return f
        return round(f / r) * r

    def get_uom_data(self, uom_id):
        """ Fetches basic information from DB about product.uom
            Caches as needed
            TODO: Cache expiry when DB changes
        """
        if uom_id not in self.GD.setdefault('uom_data_cache',{}):
            data = self.q("""
            SELECT
                    id,
                    category_id,
                    factor,
                    rounding
            FROM
                    product_uom
            WHERE
                    id = $1
            """,["int"],[uom_id])

            self.GD['uom_data_cache'][uom_id] = data and data[0] or None

        return self.GD['uom_data_cache'][uom_id]

    def uom_convert(self, from_uom_id, qty, to_uom_id ):
        """
        """
        # Just basics where if not defined, just move along
        if not from_uom_id \
           or not qty \
           or not to_uom_id \
           or from_uom_id == to_uom_id:
                return qty

        # FIXME: this is the best way?
        from_unit = self.get_uom_data(from_uom_id)
        to_unit = self.get_uom_data(to_uom_id)

        if not ( from_unit and to_unit ):
            raise Exception("Unknown UoM IDs specified")

        # Guard clause, we don't want to try and convert from a length
        # to a weight, for instance
        if from_unit['category_id'] != to_unit['category_id']:
            raise Exception('Conversion from Product UoM to Default UoM is not possible as they both belong to different Category!.')

        # Calculate the ratios and such
        amount = qty / from_unit['factor']
        amount = self.rounding(amount * to_unit['factor'], to_unit['rounding'])

        return amount

    def get_stock_locations(self):
        """ Returns a list of stock.location ids reflecting locations we consider
            "within Zaber" for the purposes of calulating values such as QoH
        """

        if 'stock_internal_locations' not in self.GD:
            # FIXME: This should be cached
            # Find all the warehouse locations
            sl_rec = self.q("""
                            SELECT      sw.id "warehouse_id",
                                        sl.id "location_id",
                                        sl.parent_left,
                                        sl.parent_right
                            FROM        stock_warehouse sw
                            LEFT JOIN   stock_location sl
                            ON          sw.lot_stock_id = sl.id
                        """,[],[])[0]

            # find the locations associated
            sl_recs = self.q("""
                          SELECT        id
                          FROM          stock_location
                          WHERE
                                        parent_left < $1
                                    AND parent_right >= $2
                        """,
                        ["integer","integer"],
                        [sl_rec['parent_right'], sl_rec['parent_left']])
            sl_ids = []
            for sl_rec in sl_recs:
                sl_ids.append(sl_rec['id'])
            self.GD['stock_internal_locations'] = sl_ids

        return self.GD['stock_internal_locations']

    def get_products_available(self, product_ids):
        """ Returns a hash of product quantities available
            We sum all the in/out moves as two different queries. Note that
            we have to handle UoM after the fact as the database doesn't
            necessarily know how to multiply/etc
        """

        internal_location_ids = tuple(self.get_stock_locations())
        product_id_list = tuple(map(int,product_ids))

        counts = self.q("""
            SELECT
                                c.product_id,
                                direction,
                                SUM(fn_uom_convert(c.product_uom,c.product_qty,pt.uom_id)) product_qty,
                                c.state
            FROM (
                                -- This query takes the various states and sums up the values
                                -- of all the stock move lines
                                select
                                        SUM(product_qty) product_qty,
                                        CASE
                                            WHEN location_id NOT IN ({internal_location_ids})
                                                THEN 'in'
                                            ELSE 'out'
                                        END direction,
                                        product_id,
                                        product_uom,
                                        state
                                from
                                        stock_move
                                where
                                    (( -- incoming moves
                                                location_id NOT IN ({internal_location_ids})
                                            AND location_dest_id IN ({internal_location_ids})
                                        )
                                        OR
                                     ( -- outgoing moves
                                                location_id IN ({internal_location_ids})
                                            AND location_dest_id NOT IN ({internal_location_ids})
                                    ))
                                    and product_id IN ({product_ids})
                                    and state IN ('confirmed','waiting','assigned','done')
                                group by
                                    product_id,
                                    product_uom,
                                    direction,
                                    state
                            ) as c
                LEFT JOIN
                            product_product pp
                        ON  pp.id = c.product_id
                LEFT JOIN
                            product_template pt
                        ON  pt.id = pp.product_tmpl_id
            GROUP BY
                c.product_id, c.direction, c.state
        """.format(
            product_ids=",".join(map(str,product_id_list)),
            internal_location_ids=",".join(map(str,internal_location_ids)),
        ))


        by_product_id = {}
        for product_id in product_id_list:
            by_product_id[product_id] = {
                                            'qty_available': 0,
                                            'virtual_available': 0,
                                            'incoming_qty': 0,
                                            'outgoing_qty': 0,
                                        }

        for count in counts:
            product_id = count['product_id']
            product_result = by_product_id[product_id]

            if count['direction'] == 'in':
                quantity = count['product_qty']
            else:
                quantity = -count['product_qty']

            if count['state'] == 'done':
                product_result['qty_available'] += quantity
                product_result['virtual_available'] += quantity

            else:
                product_result['virtual_available'] += quantity
                if count['direction'] == 'out':
                    product_result['outgoing_qty'] += quantity
                else:
                    product_result['incoming_qty'] += quantity

        return by_product_id

    def get_product_available(self, product_id):
        results = self.get_products_available([product_id])
        return pprint.pformat(results[product_id])

    def sync_product_product_summary(self):
        """ Clean up any dirty entries found in the summary table
        """

        # Ensure all dirty tracker entries are present
        self.q("""
            INSERT  INTO product_product_dirty_tracker
            ( id, qty_dirty )
                SELECT      pp.id as id,'t'as qty_dirty
                FROM        product_product pp
                  LEFT JOIN product_product_dirty_tracker ppdt
                         ON pp.id = ppdt.id
                      WHERE ppdt.id is null
        """)

        # Ensure all dirty tracker entries reflect existing products
        self.q("""
            DELETE FROM     product_product_dirty_tracker
            WHERE
                            id NOT IN(
                                SELECT  id
                                FROM    product_product
                            )
        """)

        # Deal with the dirty product counts
        # We will process in batches to reduce memory impact
        cur = self.plpy.cursor("""
                    SELECT  id
                    FROM    product_product_dirty_tracker
                    WHERE   qty_dirty = 't'
                """)
        while True:
            rows = cur.fetch(100)
            if not rows:
                break
            self.plpy.info("Syncing {} record(s)...".format(len(rows)))
            product_ids = list(map(lambda a:a['id'], rows))
            product_counts = self.get_products_available(product_ids)
            self.GD['product_counts'] = product_counts

            self.q("""
                UPDATE      product_product
                SET
                            cached_qty_available=fn_get_cached_available_qty(id),
                            cached_virtual_available=fn_get_cached_virtual_available(id),
                            cached_incoming_qty=fn_get_cached_incoming_qty(id),
                            cached_outgoing_qty=fn_get_cached_outgoing_qty(id)
                WHERE
                            id IN ({product_ids})
            """.format(
                product_ids=",".join(map(str,product_ids)),
            ))

            self.q("""
                UPDATE      product_product_dirty_tracker
                SET         qty_dirty = 'f'
                WHERE       id IN ({product_ids})
            """.format(
                product_ids=",".join(map(str,product_ids)),
            ))

        return "OK"

    def trigger_stock_move_update_row(self):
        """ This trigger should execute when a stock.move is created.
            The purpose of this function is to flag in the
            product_product table what records will need quantity
            recalculated
        """
        old = self.TD['old']
        new = self.TD['new']
        dirty_product_ids = []
        for product_id in [ old['product_id'], new['product_id'] ]:
            if not product_id: continue
            dirty_product_ids.append(product_id)

        if dirty_product_ids:
            self.q("""
                UPDATE  product_product_dirty_tracker
                SET
                        qty_dirty='t'
                WHERE
                        id in ({dirty_product_ids})
            """.format(
                dirty_product_ids=",".join(map(str,dirty_product_ids))
            ))

"""

CREATE OR REPLACE FUNCTION fn_uom_convert(from_uom_id integer,qty numeric,to_uom_id integer)
RETURNS NUMERIC AS
$$
    from izaber.plpython.zerp import init_plpy
    iplpy = init_plpy(globals(),reload=True)
    return iplpy.uom_convert(from_uom_id,qty,to_uom_id)
$$
LANGUAGE plpython3u;
select fn_uom_convert(37,1,1);


CREATE OR REPLACE FUNCTION fn_get_stock_locations()
RETURNS INT[] AS
$$
    from izaber.plpython.zerp import init_plpy
    iplpy = init_plpy(globals())
    return iplpy.get_stock_locations()
$$
LANGUAGE plpython3u;
select fn_get_stock_locations();


CREATE OR REPLACE FUNCTION fn_get_product_available(product_id integer)
RETURNS TEXT AS
$$
    from izaber.plpython.zerp import init_plpy
    iplpy = init_plpy(globals())
    return iplpy.get_product_available(product_id)
$$
LANGUAGE plpython3u;
select fn_get_product_available(1633);





DROP TABLE product_product_summary;
CREATE TABLE product_product_summary (
    id integer primary key REFERENCES product_product ON DELETE CASCADE,
    default_code varchar(64),
    name varchar(128),
    revision integer,
    active boolean,
    sale_ok boolean,
    purchase_ok boolean,
    list_price numeric(16,5),
    type varchar(16),
    loc_rack varchar(255),
    bom text,
    bom_dirty boolean,

    qty_available numeric(16,5),
    virtual_available numeric(16,5),
    incoming_qty numeric(16,5),
    outgoing_qty numeric(16,5),
    qty_dirty boolean
);
CREATE INDEX pps_qty_dirty_ndx ON product_product_summary ( qty_dirty );
CREATE INDEX pps_bom_dirty_ndx ON product_product_summary ( bom_dirty );
CREATE INDEX pps_active_ndx ON product_product_summary ( active );
CREATE INDEX pps_basic_search_ndx ON product_product_summary ( default_code, revision, active, type );



CREATE OR REPLACE FUNCTION fn_trigger_stock_move_update_row()
RETURNS TRIGGER AS
$$
    from izaber.plpython.zerp import init_plpy
    iplpy = init_plpy(globals())
    return iplpy.trigger_stock_move_update_row()
$$
LANGUAGE plpython3u;

CREATE TRIGGER      trig_default_code
AFTER UPDATE OF     product_uom,
                    product_qty,
                    location_id,
                    location_dest_id,
                    product_id,
                    state
ON
                    stock_move
FOR EACH ROW
EXECUTE PROCEDURE   fn_trigger_stock_move_update_row()
;



DROP FUNCTION fn_get_cached_available_qty(product_id integer);
CREATE OR REPLACE FUNCTION fn_get_cached_available_qty(product_id integer)
RETURNS NUMERIC AS
$$
    return GD['product_counts'].get(product_id,{}).get('qty_available')
$$
LANGUAGE plpython3u;

DROP FUNCTION fn_get_cached_virtual_available(product_id integer);
CREATE OR REPLACE FUNCTION fn_get_cached_virtual_available(product_id integer)
RETURNS NUMERIC AS
$$
    return GD['product_counts'].get(product_id,{}).get('virtual_available')
$$
LANGUAGE plpython3u;

DROP FUNCTION fn_get_cached_incoming_qty(product_id integer);
CREATE OR REPLACE FUNCTION fn_get_cached_incoming_qty(product_id integer)
RETURNS NUMERIC AS
$$
    return GD['product_counts'].get(product_id,{}).get('incoming_qty')
$$
LANGUAGE plpython3u;

DROP FUNCTION fn_get_cached_outgoing_qty(product_id integer);
CREATE OR REPLACE FUNCTION fn_get_cached_outgoing_qty(product_id integer)
RETURNS NUMERIC AS
$$
    return GD['product_counts'].get(product_id,{}).get('outgoing_qty')
$$
LANGUAGE plpython3u;



CREATE OR REPLACE FUNCTION fn_sync_product_product_summary()
RETURNS TEXT AS
$$
    from izaber.plpython.zerp import init_plpy
    iplpy = init_plpy(globals())
    return iplpy.sync_product_product_summary()
$$
LANGUAGE plpython3u;



"""
