const util = require('util');
const {
  Schema,
  Types: { ObjectId },
} = (mongoose = require('mongoose'));

const passthrough = x => x;

// Non-recursive, should be called on every level
function makeANDexplicit(args) {
  if (Array.isArray(args)) {
    return { AND: args };
  }

  return args;
}

// NOTE: Doesn't check for implicit AND
function invariantANDOR(args) {
  if (args.AND) {
    // AND
    if (args.OR) {
      throw new Error('Cannot combine AND + OR on same level:', args);
    }
  } else if (args.OR) {
    if (args.AND) {
      throw new Error('Cannot combine OR + AND on same level:', args);
    }
  }
}

function expressionPipeline(
  { path, query, modifiers = {} },
  { model, joinPathName },
) {
  let rel;
  const many = model.schema.path(path).$isMongooseArray;
  if (many) {
    rel = mongoose.model(model.schema.path(path).caster.options.ref);
  } else {
    rel = mongoose.model(model.schema.path(path).options.ref);
  }

  return [
    {
      // JOIN
      $lookup: {
        // the MongoDB name of the collection - this is potentially different
        // from the Mongoose name due to pluralization, etc. This guarantees the
        // correct name.
        from: rel.collection.name,
        as: joinPathName,
        let: { [path]: `$${path}` },
        pipeline: [
          {
            $match: {
              ...query,
              $expr: { [many ? '$in' : '$eq']: ['$_id', `$$${path}`] },
            },
          },
        ],
      },
    },
    // Filter out empty array results (the $lookup will return a document with
    // an empty array when no matches are found in the related field
    // TODO: This implies a `some` filter. For an `every` filter, we would need
    // to somehow check that the resulting size of `path` equals the original
    // document's size. In the case of one-to-one, we'd have to check that the
    // resulting size is exactly 1 (which is the same as `$ne: []`?)
    { $match: { [joinPathName]: { $exists: true, $ne: [] } } },
  ];
}

function postAggregateMutationFactory({ path }, { model, joinPathName }) {
  let rel;
  const many = model.schema.path(path).$isMongooseArray;
  if (many) {
    rel = mongoose.model(model.schema.path(path).caster.options.ref);
  } else {
    rel = mongoose.model(model.schema.path(path).options.ref);
  }

  // Recreate Mongoose instances of the sub items every time to allow for
  // further operations to be performed on those sub items via Mongoose.
  return item => {
    if (!item) {
      return;
    }

    let joinedItems;

    if (many) {
      joinedItems = item[path].map((itemId, itemIndex) => {
        const joinedItemIndex = item[joinPathName].findIndex(({ _id }) => _id.equals(itemId));

        if (joinedItemIndex === -1) {
          return itemId;
        }

        // Extract that element out of the array (so the next iteration is a bit
        // faster)
        const joinedItem = item[joinPathName].splice(joinedItemIndex, 1)[0];
        return rel(joinedItem);
      });

      // At this point, we should have spliced out all of the items
      if (item[joinPathName].length > 0) {
        // I don't see why this should ever happen, but just in case...
        throw new Error(
          `Expected results from MongoDB aggregation '${joinPathName}' to be a subset of the original items, bet left with:\n${JSON.stringify(item[joinPathName])}`,
        );
      }
    } else {
      const joinedItem = item[joinPathName][0];
      if (!joinedItem || !joinedItem._id.equals(item[path])) {
        // Shouldn't be possible due to the { $exists: true, $ne: [] }
        // aggregation step above, but in case that fails or doesn't behave
        // as expected, we can catch that now.
        throw new Error(
          'Expected MongoDB aggregation to correctly filter to a single related item, but no item found.',
        );
      }
      joinedItems = rel(joinedItem);
    }

    const newItemValues = {
      ...item,
      [path]: joinedItems,
    };

    // Get rid of the temporary data key we used to join on
    // Should be an empty array now that we spliced all the values out above
    delete newItemValues[joinPathName];

    return newItemValues;
  };
}

function buildPipeline(args, { model }, pipeline = {}) {
  args = makeANDexplicit(args);
  invariantANDOR(args);

  function buildIt(arg) {
    // An expression
    const joinPathName = `${arg.path}__matched`;
    pipeline.pipeline = (pipeline.pipeline || []).concat(
      expressionPipeline(arg, { joinPathName, model }),
    );
    pipeline.postAggregateMutation = (
      pipeline.postAggregateMutation || []
    ).concat(postAggregateMutationFactory(arg, { joinPathName, model }));
  }

  // TODO
  if (args.AND) {
    // TODO: Recurse / nest
    args.AND.forEach(buildIt);
  } else if (args.OR) {
    // If there are more than one path/query combo, we can do them with `$facet`
    // aggregate state
    throw new Error('OR not implemented');
  } else {
    // An expression
    buildIt(args);
  }

  return pipeline;
}

/**
 * @param args {Object} Recursive format: <EXPRESSION | AND>
 * Where:
 *
 * <EXPRESSION>:
 * <AND | OR | { path, query }>
 *
 * <OR>:
 * {
 *   OR: [
 *     <AND | OR | EXPRESSION>,
 *     <AND | OR | EXPRESSION>,
 *     ...
 *   ]
 * }
 *
 * <AND>:
 * <IMPLICIT_AND | EXPLICIT_AND>
 *
 * <IMPLICIT_AND>:
 * [
 *   <AND | OR | EXPRESSION>,
 *   <AND | OR | EXPRESSION>,
 *   ...
 * ]
 *
 * <EXPLICIT_AND>:
 * {
 *   AND: [
 *     <AND | OR | EXPRESSION>,
 *     <AND | OR | EXPRESSION>,
 *     ...
 *   ]
 * }
 */
function lookup(args) {
  /*
  if (modifiers.some) {
    // $expr: { $in: ["$_id", `$$${path}`] }
  } else if (modifiers.every) {
    // ??
    // 
  }
  */
  const pipeline = buildPipeline(args, { model: this });

  return this.aggregate([
    /* OR
     *
    {
      $facet: {
        firstFacet: pipeline1.pipeline,
        secondFacet: pipeline2.pipeline,
      },
    }, {
      $project: {
        'result_of_query': {
          // TODO: FIXME: Dedupe
          $setUnion: [ '$firstFacet', '$secondFacet' ],
        },
        'firstFacet': 1,
        'secondFacet': 1
      }
    }, {
      // $unwind + $replaceRoot to return a flattened array
      $unwind: '$result_of_query',
    }, {
      $replaceRoot: {
        newRoot: '$result_of_query'
      }
    },
    */
    ...pipeline.pipeline,
  ])
    .exec()
    .then(data =>
      data
        .map((item, index, list) => (
          // Iterate over all the mutations
          pipeline.postAggregateMutation.reduce(
            // And pass through the result to the following mutator
            (mutatedItem, mutation) => mutation(mutatedItem, index, list),
            // Starting at the original item
            item,
          )
        ))
        // If anything gets removed, we clear it out here
        .filter(Boolean),
    )
    .then(data => {
      console.log(util.inspect(data, { depth: null, colors: true }));
      return data;
    });
}

const uri = 'mongodb://localhost/looktest';

mongoose.Promise = global.Promise;
//mongoose.set('debug', true);

const userSchema = new Schema({
  name: String,
});

const statusSchema = new Schema({
  status: String,
});

const categorySchema = new Schema({
  name: String,
});

const postSchema = new Schema(
  {
    title: String,
    author: { type: Schema.Types.ObjectId, ref: 'User' },
    status: { type: Schema.Types.ObjectId, ref: 'Status' },
    categories: [{ type: Schema.Types.ObjectId, ref: 'Category' }],
  },
  { timestamps: true },
);

postSchema.statics.lookup = lookup;

const User = mongoose.model('User', userSchema);
const Status = mongoose.model('Status', statusSchema);
const Category = mongoose.model('Category', categorySchema);
const Post = mongoose.model('Post', postSchema);

(async function() {
  try {
    const conn = await mongoose.connect(uri);

    // Clean data
    await Promise.all(Object.entries(conn.models).map(([k, m]) => m.remove()));

    // Create items
    const users = await User.insertMany(
      ['Jed Watson', 'Jess Telford', 'Boris Bozic'].map(name => ({ name })),
    );

    const statuses = await Status.insertMany(
      ['Deleted', 'Draft', 'Published', 'Archived'].map(status => ({ status })),
    );

    const categories = await Category.insertMany(
      ['React', 'GraphQL', 'node', 'frontend'].map(name => ({ name })),
    );

    await Post.create({
      title: 'Something',
      author: users[1],
      status: statuses[0],
      categories: [categories[0]],
    });

    await Post.create({
      title: 'An Article',
      author: users[1],
      status: statuses[2],
      categories: [categories[0], categories[1]],
    });

    await Post.create({
      title: 'And another thing!',
      author: users[1],
      status: statuses[3],
      categories: [categories[1], categories[2]],
    });

    await Post.create({
      title: 'Oh hi there...',
      author: users[2],
      status: statuses[3],
      categories: [categories[2], categories[3]],
    });

    await Post.create({
      title: 'Talkin bout Keystone',
      author: users[0],
      status: statuses[1],
      categories: [categories[0], categories[2]],
    });

    await (async () => {
      const postAuthorNames = [/Jess/i];
      console.log(`Lookup posts with Author ${postAuthorNames}`);
      await Post.lookup({
        path: 'author',
        query: { name: { $in: postAuthorNames } },
      });
    })();

    await (async () => {
      const postCategoryNames = ['React', 'GraphQL'];
      console.log(`Lookup posts with Categories ${postCategoryNames}`);
      await Post.lookup({
        path: 'categories',
        query: { name: { $in: postCategoryNames } },
      });
    })();

    await (async () => {
      const postAuthorNames = [/Je/i];
      const postCategories = ['node'];
      const postStatuses = ['Published', 'Archived'];
      console.log(
        `Lookup posts with Status ${postStatuses}, and Author ${postAuthorNames}, and Categories ${postCategories}`,
      );
      // Implicit 'AND' query
      await Post.lookup([
        {
          path: 'status',
          query: { status: { $in: postStatuses } },
        },
        {
          path: 'author',
          query: { name: { $in: postAuthorNames } },
        },
        {
          path: 'categories',
          query: { name: { $in: postCategories } },
        },
      ]);
    })();

    await mongoose.disconnect();
  } catch (e) {
    console.error(e);
  } finally {
    process.exit();
  }
})();
